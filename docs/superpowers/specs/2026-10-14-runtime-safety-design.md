# Runtime safety design (WP 1.5a)

Date: 2026-10-14. Status: approved in conversation (sections 1-3), pending written-spec review.
Revised 2026-10-14 after the plan dry run:
- `engine.dispose` gets a 1 s floor after the deadline, still inside the watchdog grace;
- Ctrl-C while waiting for the lock exits 130 quietly;
- a slow migration interrupted by a signal is ended by the watchdog at T + 5 s, and SQLite rolls back the unfinished migration transaction.

Revised 2026-10-14 after the PR #23 spec review:
- the lock is taken in the CLI before the migration;
- the journal path is canonicalised;
- the second signal is a direct process exit;
- one shutdown coordinator serves every exit cause;
- (round 2) the watchdog and deadline are armed synchronously in the signal handler, before any event-loop work;
- snapshot freshness is tracked separately from loop liveness.
Sources: PLAN §3.2 items 7 and 8, PLAN 1.5 row, D7, D15; SPEC §0, §4.11;
`core/runtime.py`, `core/cycle.py` (`CLOSE_BUDGET_S`), `cli.py`.

WP 1.5 is split into four sub-PRs (owner, 2026-10-14):
- **1.5a, runtime safety (this spec):** items 7 and 8.
- **1.5b, fill and position accounting:** items 3 and 6, plus the 1.R2 follow-ups.
- **1.5c, exit management:** items 2 and 5.
- **1.5d, startup recovery and reconciliation:** items 1 and 4, plus wiring the Cycle Runner into `run core`.

## 1. Purpose and success criteria

`run core` must be safe to leave running before any position logic exists. Today:
- Kill detection runs inline after `feed.tick()`, so a hung REST refresh also stalls kill detection.
- Nothing stops a second core from writing the same journal.
- Shutdown is unbounded, and nothing handles a wedged event loop.

Done means:
- **One writer per journal.** Only one `run core` process can hold a given journal, identified by its canonical file path. A second one waits a bounded time, then fails startup with the holder's PID. It never migrates or opens the journal before it holds the lock.
- **Independent loops.** Kill/control polling, feed ticks and equity snapshots run as independent supervised loops, and a hung step in one never delays the others. Stalls are logged. `risk.snapshot_overdue` is logged when the last *committed* snapshot is older than 2 x its interval.
- **Bounded shutdown.** Every way the core stops (first SIGINT/SIGTERM, programmatic stop, startup failure, stream death) runs one coordinated teardown with two bounds:
  - **graceful:** teardown is abandoned at `shutdown.timeout_s` (T) and the process exits 1;
  - **last resort:** a watchdog thread ends the process at T + 5 s if even that hangs.
  - A second signal ends the process at once.
- No behaviour change for entries, exits or the feed's data handling.

Owner decisions taken while designing (2026-10-14, recorded in PLAN 1.5):
- The lock file is **derived from the database path**, with no new config key.
- Shutdown uses a **graceful budget, a second-signal hard stop and a watchdog thread**.

## 2. Single-writer lock (`core/lock.py`)

- **Path:** derived from the journal's **canonical** file path, with the lock file next to it: SQLite `.../data/autotrader.db` gives `.../data/autotrader.db.lock`.
  - `config.py`'s `_resolve_sqlite_url` already makes a relative path absolute against `config.yaml`'s directory. The lock code then applies `os.path.realpath`, which resolves symlinks in every path component, including a symlinked database file.
  - The same canonical path is used for the URL that opens the journal (migration and engine). Two configs that reach one journal through symlinks therefore contend for the same lock.
  - **Hard links are unsupported:** two hard-linked names are separate paths to the same inode. The spec states this limit rather than detecting it.
  - The lock lives in a separate file, not on the database itself, because SQLite's own POSIX locks on the database file may interact with `flock` on macOS.
- **Non-file databases:** any URL that is not a file-backed SQLite database fails startup with `single-writer lock needs a file journal; cross-host ownership is WP 3.5`. That includes Postgres and in-memory SQLite.
- **Mechanism:** an exclusive, non-blocking `fcntl.flock` on the lock file, which is opened or created.
  - The file descriptor stays open until the process exits, and process death is what releases the lock: a clean exit, a crash, `kill -9` or `os._exit`.
  - There is **no explicit early release** on any production path. A cancelled aiosqlite caller can leave its queued write running in the worker thread, so closing the descriptor before the process ends could let another core in while this one still writes. Only tests release explicitly, after their writers have finished.
  - After acquiring the lock, the file is truncated and the PID and the start time (UTC ISO) are written. These are diagnostic only; the lock is what excludes other processes.
- **When:** in the CLI's `run core` command, after config validation and **before `db.upgrade()`**, i.e. before anything migrates, opens or writes the journal.
  - The held lock is passed to `core.runtime.run_core(..., lock=...)`. The runtime never acquires a second lock; it checks that it was given a held one.
  - Failures after acquisition (migration, bar source, startup) exit the process, and that releases the lock.
- **Contention:**
  - A second core retries the non-blocking lock once per second for `shutdown.timeout_s + 5` seconds, the old process's last-resort bound. That way a restart can begin while the previous process is still shutting down. The lock is released only when the old process has exited, so its SQLite work has been committed or rolled back.
  - On timeout, startup fails with `journal locked by PID <pid> since <ts>` and exit code 1. The losing core never calls `db.upgrade()` and never opens the journal.
  - Ctrl-C while waiting exits 130 with a one-line message and no traceback. Nothing is owned yet, and the handlers are installed only after the lock.
- **Scope:** only `run core` takes the lock. `autotrader kill`, `check-config` and `db upgrade` don't, so the kill switch keeps working while a core runs.
- **Platform:** macOS and Linux (`fcntl`). Windows is not supported.

## 3. Supervised loops (`core/supervise.py`)

- **`Loop(name, step, interval_s, stall_after_s)`:** the supervisor calls `await step()`, then sleeps for the rest of `interval_s`. The time it records is when `step()` last **completed**: the loop's heartbeat.
- **Stall detection:**
  - A check that runs in the supervisor itself (not inside the loop's task) logs `<name>.stalled` with the heartbeat age, once per stall, when the age exceeds `stall_after_s`.
  - It logs `<name>.recovered` once the heartbeat is fresh again.
  - It never cancels a slow `step()`. Cancelling could cut a journal write mid-commit, the same reasoning as today's `_snapshot_loop`.
- **Errors and task death:**
  - An exception escaping `step()` is logged (`<name>.step_failed`), and the loop carries on at its next interval.
  - A loop task that dies by an escaped `BaseException` is logged and restarted, as `run_core` does today for the snapshot task.
- **Shutdown:** `stop(deadline)` lets each loop finish its current `step()` if it can before the deadline, then cancels what is left. It returns the names of loops that had to be cancelled.
- **The market data stream** stays a plain task outside the supervisor. If it ends, `run_core` fails, as today.

Loops registered in 1.5a:

| Loop | Step | Interval | Stall after | Stall log level |
|---|---|---|---|---|
| `feed.tick` | `feed.tick()` | 1 s | 30 s | warning |
| `control.poll` | `gate.status()`; log kill/pause changes (`core.kill_detected` / `core.kill_cleared`, as today) | 1 s | 10 s | **error** |
| `risk.snapshot` | `risk.snapshot_equity()` | 60 s | 120 s (`risk.snapshot.stalled`) | warning |

- **Snapshot freshness is separate from loop liveness.** PLAN item 8 asks for the age of the last *persisted* snapshot, not the last finished loop step. The two differ:
  - A loop that keeps failing can be alive without ever writing a snapshot.
  - `RiskEngine.snapshot_equity()` commits a snapshot (equity and peak, exposure `NULL`) and *then* raises when the position read fails.
- So `RiskEngine` keeps `last_snapshot_at`, set right after each successful `_write_snapshot` commit, on both the full and the partial path. It is an attribute of the engine, so it survives loop restarts.
- The supervisor's stall check logs `risk.snapshot_overdue` once when `now - last_snapshot_at > 120 s` and `risk.snapshot_fresh` when it recovers. Before the first write, the age is measured from core start, so "no snapshot within 120 s of start" also warns.
- The `control.poll` loop keeps the latest `ControlStatus` so that 1.5c can act on kill changes without a second poller. In 1.5a, a kill is only logged, as today.
- The intervals and thresholds are constants in `runtime.py`, with no new config keys apart from `shutdown.timeout_s`.
- The entry fail-closed behaviour is unchanged: `ControlGate` already fails closed, and no entries run until 1.5d.
- The main loop becomes "supervise until stop or stream death". It raises on stream death and runs the stall check about once a second.

## 4. Bounded shutdown, second signal, watchdog

**Config:** `shutdown.timeout_s` is required in the schema: a finite number with `0 < v <= 300`. `config.yaml` sets **30**, which is above the Cycle Runner's 20 s close budget (`CLOSE_BUDGET_S`) wired in 1.5d. It is infrastructure config; the profile hash is unchanged.

**Shutdown trigger.** `begin_shutdown(cause)` is plain synchronous code, so it can't depend on the event loop running. It is shared by every exit cause: the first signal, a programmatic stop (`stop` event), a startup failure after the lock, and stream death.
- The **first** call does three things, in this order:
  1. records the cause and a **monotonic deadline**, `time.monotonic()` + T, measured from the trigger itself (for a signal, the signal);
  2. **starts the watchdog thread**;
  3. only then requests the async teardown: it sets the stop flag and wakes the loop with `call_soon_threadsafe` when a loop exists.
- Later calls are no-ops. The deadline and timer are never reset or re-armed. The second-signal path is separate (below).
- **It runs inside the Python-level signal handler.** If the event loop is stuck in synchronous Python code when the first signal arrives, the watchdog is already running and T is already counting, even though the queued teardown can't start yet.
- **Installed early.** The CLI installs the `signal.signal` handlers right after acquiring the lock, *before* `db.upgrade()` or any other owned startup work that may block.
  - A signal during the migration, before any event loop exists, records the deadline, arms the watchdog and sets the flag.
  - When `run_core` starts, it sees the flag and goes straight to teardown without starting loops or the stream.

**Shutdown coordinator.** The async `Shutdown.run()` in `runtime.py` performs the teardown.
- It reads the deadline and cause recorded by `begin_shutdown`, and never sets its own. When it is reached by a programmatic stop or stream death, it calls `begin_shutdown` first, which is a no-op if a signal already did.
- It then runs the ordered teardown:
  1. Admission: nothing to close in 1.5a. 1.5d inserts "close admission, then `runner.close()`" here.
  2. `source.stop()`, then `supervisor.stop(deadline)`.
  3. `bus.close()`, bounded by the remaining time.
  4. `engine.dispose()`, bounded by the remaining time but given **at least 1 s** even after the deadline (still inside the 5 s watchdog grace). Otherwise a stuck earlier step would leave the journal's connections open on every forced teardown.
- **The deadline is enforced by not waiting.** Cancellation alone can't enforce it: a task that catches `CancelledError` keeps `wait_for` pending. So each step is waited on with `asyncio.wait(..., timeout=remaining)`. When time runs out, the step's task is cancelled once and then *left behind*, without awaiting it again.
  - `core.shutdown_forced` is logged with the step and the loops still running, and teardown moves on.
  - The process exits 1 for a forced deadline, startup failure or stream death, and 0 for a clean stop.
- **Abandoned work** stays in the journal for 1.5d's startup recovery: open cycles, `pending`/`unknown` orders, and any queued aiosqlite write that may still finish. It is logged as `core.shutdown_abandoned`. Forced cancellation does not prove that broker work stopped.
- **After `run_core` returns:** `asyncio.run` teardown can itself block on tasks or executor work that were left behind. The watchdog stays armed through it (see below), so the process still ends by T + 5.

**Second signal (hard):** the CLI installs the second-signal path with `signal.signal`, the Python-level handler, not the event loop's `add_signal_handler`.
- The first signal calls `begin_shutdown("signal")` synchronously in the handler (watchdog armed, deadline recorded, then the async teardown is queued). The second calls `os._exit(130)` directly in the handler.
- There is no async cleanup, no engine dispose and no explicit lock release. Process death releases the lock, so no other core can start while a queued write of this one is still running.
- Python-level handlers run in the main thread between bytecodes, so this works while the event loop is stuck in Python code. A stall inside a C call is the watchdog's job.

**Watchdog (last resort):**
- `begin_shutdown` arms it on its first call, for **every** exit cause, through the CLI's callback, before any event-loop work. It is a daemon `threading.Timer` for T + 5 s from the trigger time, armed once. The thread is needed because a timer on the event loop can't fire while the loop is blocked.
- If the timer fires, it writes `core.watchdog_exit` to stderr directly (not through the event loop) and calls `os._exit(70)`.
- It is cancelled only after `asyncio.run(...)` has fully returned, immediately before the CLI's normal exit, so it covers asyncio and executor teardown too.

| Exit code | Meaning |
|---|---|
| 0 | clean stop |
| 1 | startup failure (including lock timeout), stream death, or a graceful deadline (T) that forced teardown |
| 130 | second signal |
| 70 | watchdog (last resort, T + 5 s) |

## 5. Testing

- **Lock:**
  - A second acquisition from a **subprocess** times out with the holder's PID in the error. A short wait is injected; no long real sleeps.
  - Acquisition succeeds after the holder exits.
  - The derived path for a SQLite URL is correct and canonical: a **symlinked database file and a symlinked directory** contend with the real path (subprocess).
  - Postgres, `:memory:` and other non-file URLs fail with the documented error.
  - **CLI level:** while a subprocess holds the lock, a second `run core` exits 1, and `db.upgrade` and the engine are never called. Both are injected and asserted not called.
  - Ownership is released only by process exit: after a forced shutdown and after a second signal, a fresh process can acquire the lock only once the old one has exited.
- **Supervisor**, driven by an injected clock and events, never by wall-clock assertions (lesson from PR #13):
  - Stall and recovery logging happens once per transition.
  - `risk.snapshot_overdue` is driven by `last_snapshot_at`, not by the loop heartbeat:
    - Repeated account-read failures (no write) log it once age > 120 s, even though the loop keeps running.
    - A position-read failure that still commits equity/HWM refreshes `last_snapshot_at`, so no overdue warning.
    - Before the first write, the age is measured from start.
  - A raising `step()` keeps its loop running.
  - A dead task is restarted.
  - `stop(deadline)` cancels the steps that are still hanging and returns their names.
- **Independence:** a fake bar source whose REST `latest()` hangs forever. A kill-file change is still logged by `control.poll`, and `feed.tick.stalled` is logged.
- **Shutdown:**
  - A **cancellation-resistant** step (catches `CancelledError` and keeps waiting) and a hanging `bus.close()` are left behind at the deadline: teardown completes, exit 1, and `core.shutdown_forced` is logged.
  - **Stream death followed by cancellation-resistant cleanup** goes through the same coordinator: the deadline holds, the watchdog is armed, exit 1.
  - A **pending aiosqlite worker write** at a forced shutdown: the lock is not released by the process (a fresh acquirer in a subprocess fails until the process exits).
  - The second-signal handler calls the injected exit function with 130, without awaiting anything, and also while the loop is blocked.
  - Re-entering the coordinator (a signal during stream-death teardown) is a no-op apart from the second-signal path.
  - **First signal while the loop is blocked:** the handler is invoked directly while the test holds the loop in synchronous code. The injected timer must already be armed and the deadline recorded **before any loop callback has run**. When the loop is released, the coordinator uses that same deadline, and its remaining time is measured from the signal, not from coordinator entry.
  - **Signal during `db.upgrade`:** the injected `db.upgrade` delivers the signal. The watchdog is armed at once, `run_core` goes straight to teardown, and no loops or stream are started.
  - A later `begin_shutdown` call (a stream death after a signal) doesn't reset the deadline or start a second timer.
- **Watchdog:** with the timer factory and exit function injected:
  - It is armed on every exit cause.
  - It fires once at T + 5.
  - It is cancelled only after `asyncio.run` returns cleanly.
  - Tests never call the real `os._exit`.
- **Config:** a missing `shutdown.timeout_s`, 0, a negative value, NaN, infinity and 301 are all rejected; 30 loads.

## 6. Out of scope

- Cross-host or Postgres ownership and persistent account binding (WP 3.5).
- Acting on a kill: cancelling entries and flatten (1.5c).
- Wiring the runner, closing admission, startup recovery (1.5d).
- Windows support.

## 7. Documentation

- SPEC §4.11: add the single-writer lock and bounded shutdown to the cross-cutting rules.
- `config.yaml` and its schema: `shutdown.timeout_s`.
- The `core/runtime.py` module docstring: supervised loops, lock, shutdown.
- PLAN: a 1.5a done-note on the 1.5 row (append inside the last cell), and the repo-state line.
- The CLAUDE.md commands list: `run core` single-writer lock, and "Ctrl-C stops (twice = hard stop)".
