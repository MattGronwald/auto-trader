# Runtime safety design (WP 1.5a)

Date: 2026-10-14. Status: approved (three Codex spec rounds and two plan rounds on PR #23), then revised by the self-review below.

Revised 2026-10-14 after a self-review against the code, before implementation:
- the watchdog is never cancelled once armed, because interpreter exit can hang on non-daemon aiosqlite threads;
- startup races the stop request, so a signal during the broker connect or the backfill is bounded too;
- a separate `control.kill_file` loop checks the kill file without the journal;
- `core.failed` logs startup failures and stream death with the traceback;
- item 8's "fail entry checks closed" is explicitly deferred to 1.5d (section 6);
- the limits of Python-level signal handlers are stated, and the section 5 tests match the plan.

Revised 2026-10-14 after the Codex review of that revision (PR #23):
- an unavailable journal read no longer clears a known kill (`ControlStatus.available`);
- the watchdog's limit is qualified: it can't run while a native call holds the GIL.
Revised 2026-10-14 after the second Codex plan review: the watchdog fires `log_wait_s` early so the exit, including the diagnostic wait, stays within T + 5 s.

Revised 2026-10-14 after the Codex plan review:
- the watchdog's stderr line is best-effort and bounded, and the exit never waits for it;
- a teardown step that raises or is cancelled makes the exit 1 (`core.shutdown_failed`), and the remaining steps still run;
- loop cadence is start-to-start with no catch-up.

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
- **Independent loops.** Kill/control polling, a journal-free kill-file check, feed ticks and equity snapshots run as independent supervised loops, and a hung await in one never delays the others. They still share the event loop and the journal: a step that blocks the event loop synchronously stalls all of them, and a hung journal stalls every loop that reads it, which is why the kill file has its own loop. Stalls are logged. `risk.snapshot_overdue` is logged when the last *committed* snapshot is older than 2 x its interval.
- **Bounded shutdown.** Every way the core stops (first SIGINT/SIGTERM, programmatic stop, startup failure, stream death), including a stop requested during startup, runs one coordinated teardown with two bounds:
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

- **`Loop(name, step, interval_s, stall_after_s)`:** the supervisor calls `await step()`, then sleeps for the rest of `interval_s`, measured from the step's start on the supervisor's clock. A step that overruns its interval is followed immediately by the next one, with no catch-up. The time it records is when `step()` last **completed**: the loop's heartbeat.
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
| `control.kill_file` | the kill file only (sync, no journal); raises the kill, never clears it | 1 s | 10 s | **error** |
| `risk.snapshot` | `risk.snapshot_equity()` | 60 s | 120 s (`risk.snapshot.stalled`) | warning |

- **Snapshot freshness is separate from loop liveness.** PLAN item 8 asks for the age of the last *persisted* snapshot, not the last finished loop step. The two differ:
  - A loop that keeps failing can be alive without ever writing a snapshot.
  - `RiskEngine.snapshot_equity()` commits a snapshot (equity and peak, exposure `NULL`) and *then* raises when the position read fails.
- So `RiskEngine` keeps `last_snapshot_at`, set right after each successful `_write_snapshot` commit, on both the full and the partial path. It is an attribute of the engine, so it survives loop restarts.
- The supervisor's stall check logs `risk.snapshot_overdue` once when `now - last_snapshot_at > 120 s` and `risk.snapshot_fresh` when it recovers. Before the first write, the age is measured from core start, so "no snapshot within 120 s of start" also warns.
- **The kill file has its own loop.** The kill file is the fallback for a broken journal, but `gate.status()` reads the journal after checking the file, so a hung journal read would hide a kill file created meanwhile. `control.kill_file` checks only the file. Both loops update one `ControlPoller` state:
  - a kill file seen by either loop keeps the status killed;
  - only `control.poll` clears a kill, and only when the file is absent at that moment, so a journal read that started before the file appeared can't flap the status back;
  - only a **successful** journal read clears a kill. `ControlGate.status()` returns `killed=False` with a "control state unavailable" reason when the journal read fails. That blocks entries but proves no clearance. `ControlStatus` gets a typed `available` flag (default `True`, `False` on that path), and the poller keeps a known kill on an unavailable read, adding the unavailable reason so it stays visible, without logging a transition. Entry checks are unchanged.
- The poller keeps the latest `ControlStatus` so that 1.5c can act on kill changes without another poller. In 1.5a, a kill is only logged, as today. A status raised by the file loop carries only the file reason; 1.5c should act on `killed`, not on the reasons list.
- The intervals and thresholds are constants in `runtime.py`, with no new config keys apart from `shutdown.timeout_s`.
- The entry fail-closed behaviour is unchanged: `ControlGate` already fails closed, and no entries run until 1.5d.
- The main loop becomes "supervise until stop or stream death". It raises on stream death and runs the stall check about once a second.
- **Startup races the stop request.** Strategy registration, the broker connect, the backfill and the wiring run as one startup task, raced against the stop request. A signal during a long backfill therefore doesn't wait for it.
  - If the stop request comes first, the unfinished startup becomes teardown step 0 (`startup`): it gets the remaining budget, then it is cancelled once and left behind (forced, exit 1).
  - Startup checks the request again right before starting the stream and the loops, with no await in between. A startup that finishes after the request therefore starts neither, and the exit is clean.

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
  0. An unfinished startup (see section 3), bounded by the remaining time.
  1. Admission: nothing to close in 1.5a. 1.5d inserts "close admission, then `runner.close()`" here.
  2. `source.stop()`, then `supervisor.stop(deadline)`.
  3. `bus.close()`, bounded by the remaining time.
  4. `engine.dispose()`, bounded by the remaining time but given **at least 1 s** even after the deadline (still inside the 5 s watchdog grace). Otherwise a stuck earlier step would leave the journal's connections open on every forced teardown.
- **The deadline is enforced by not waiting.** Cancellation alone can't enforce it: a task that catches `CancelledError` keeps `wait_for` pending. So each step is waited on with `asyncio.wait(..., timeout=remaining)`. When time runs out, the step's task is cancelled once and then *left behind*, without awaiting it again.
  - `core.shutdown_forced` is logged with the step and the loops still running, and teardown moves on.
  - A step that completes but raises, or is cancelled, is **failed**: `core.shutdown_failed` is logged with the step names, and the remaining steps still run.
  - The process exits 1 for a forced or failed step, startup failure or stream death, and 0 only for a clean stop (`core.stopped`).
- **Abandoned work** stays in the journal for 1.5d's startup recovery: open cycles, `pending`/`unknown` orders, and any queued aiosqlite write that may still finish. It is logged as `core.shutdown_abandoned`. Forced cancellation does not prove that broker work stopped.
- **Failures are logged first.** A startup failure or stream death is logged as `core.failed`, with the cause and the traceback, *before* teardown, because the watchdog may cut teardown short. The CLI then prints a one-line summary and exits 1.
- **After `run_core` returns:** `asyncio.run` teardown can itself block on tasks or executor work that were left behind, and so can interpreter exit, which joins non-daemon threads (aiosqlite's worker threads are non-daemon). The watchdog stays armed through both (see below), so the process still ends by T + 5.

**Second signal (hard):** the CLI installs the second-signal path with `signal.signal`, the Python-level handler, not the event loop's `add_signal_handler`.
- The first signal calls `begin_shutdown("signal")` synchronously in the handler (watchdog armed, deadline recorded, then the async teardown is queued). The second calls `os._exit(130)` directly in the handler.
- There is no async cleanup, no engine dispose and no explicit lock release. Process death releases the lock, so no other core can start while a queued write of this one is still running.
- Python-level handlers run in the main thread between bytecodes, so this works while the event loop is stuck in Python code. A blocking call that is interrupted by the signal (for example `select`, `sleep` or a lock wait) runs the handler too.
- **Limit:** if the main thread is stuck in a C call that neither returns nor is interrupted, no Python-level handler runs: the first signal arms nothing and the second can't exit.
- **The watchdog has a limit too.** It is a Python thread, so once armed it covers stalls in Python code and in native waits that release the GIL (I/O, `select`, `sleep`, SQLite calls). It can't run while a native call holds the GIL and never returns.
- The T + 5 bound therefore holds only for stalls that leave the GIL free. A process stuck in a GIL-holding native call is ended only by SIGKILL or a supervisor outside the interpreter. That is out of scope here (section 6).
- A signal that arrives after another cause (stream death, startup failure, programmatic stop) began shutdown counts as the *first* signal: it is a no-op, because shutdown is already running. The next one exits 130. So when shutdown is already under way, it takes two Ctrl-C to force an exit.

**Watchdog (last resort):**
- `begin_shutdown` arms it on its first call, for **every** exit cause, through the CLI's callback, before any event-loop work. It is a daemon `threading.Timer`, armed once, that ends the process by T + 5 s from the trigger time, unless a native call holds the GIL (see the limit under "Second signal"). It fires 0.5 s early (the diagnostic wait below), so the wait is inside the bound. The thread is needed because a timer on the event loop can't fire while the loop is blocked.
- If the timer fires, it calls `os._exit(70)` in all cases. Before that it tries to write `core.watchdog_exit` to stderr (directly, not through the event loop). The write runs in a separate daemon thread that gets at most 0.5 s and whose errors are ignored, so a closed, broken or blocking stderr can't keep the process alive.
- **It is never cancelled.** It is a daemon thread, so it never delays a normal exit. It must outlive `asyncio.run(...)`, because interpreter exit joins non-daemon threads: a scratch run showed that an unclosed, still-referenced aiosqlite connection keeps the process alive after `asyncio.run` returns, and that the armed watchdog then ends it with 70. A clean exit that hangs there is therefore ended too.

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
- **Lock across a stuck shutdown** (real handlers, in a subprocess): after the first signal a second acquirer still fails; after the second signal the process exits 130 and the lock can be taken.
- **Supervisor**, driven by an injected clock and events, never by wall-clock assertions (lesson from PR #13):
  - Stall and recovery logging happens once per transition.
  - `risk.snapshot_overdue` is driven by `last_snapshot_at`, not by the loop heartbeat:
    - Repeated account-read failures (no write) log it once age > 120 s, even though the loop keeps running.
    - A position-read failure that still commits equity/HWM refreshes `last_snapshot_at`, so no overdue warning.
    - Before the first write, the age is measured from start.
  - A raising `step()` keeps its loop running.
  - A dead task is restarted.
  - `stop(deadline)` cancels the steps that are still hanging and returns their names.
- **Independence:**
  - A fake bar source whose REST `latest()` hangs forever: `feed.tick.stalled` is logged, and a kill file created afterwards is still logged as `core.kill_detected`.
  - A hung journal read in `gate.status()`: `control.poll.stalled` is logged, and a kill file created afterwards is still detected by `control.kill_file`.
  - The poller never clears a kill while the kill file is present, even when a journal read returns "not killed".
  - A known kill followed by an unavailable journal read stays killed, with the unavailable reason, no log and no growth of the reasons on repeated outages. A later successful read with no file clears it, logging `core.kill_cleared` once.
- **Startup:**
  - A stop request during a hung backfill is forced at the deadline (`startup`, exit 1), and `core.started` is never logged.
  - A backfill that finishes after the stop request starts no stream or loop, and the exit is clean.
- **Shutdown:**
  - A **cancellation-resistant** step (catches `CancelledError` and keeps waiting) and a hanging `bus.close()` are left behind at the deadline: teardown completes, exit 1, and `core.shutdown_forced` is logged.
  - **Stream death followed by cancellation-resistant cleanup** goes through the same coordinator: the deadline holds, the watchdog is armed, exit 1. Stream death is logged as `core.failed` with the error.
  - `run_core` never releases the lock, including after a forced teardown (`lock.held` stays true). The earlier idea of a separate test with a pending aiosqlite write is dropped: the lock is a descriptor that nothing on a production path closes, so a pending write can't affect it. The subprocess test above covers release on process death.
  - The second-signal handler calls the injected exit function with 130, without awaiting anything, and also while the loop is blocked.
  - Re-entering the coordinator (a signal during stream-death teardown) is a no-op apart from the second-signal path.
  - **First signal while the loop is blocked:** the handler is invoked directly while the test holds the loop in synchronous code. The injected timer must already be armed and the deadline recorded **before any loop callback has run**. When the loop is released, the coordinator uses that same deadline, and its remaining time is measured from the signal, not from coordinator entry.
  - **Signal during `db.upgrade`:** the injected `db.upgrade` delivers the signal. The watchdog is armed at once, `run_core` goes straight to teardown, and no loops or stream are started.
  - A later `begin_shutdown` call (a stream death after a signal) doesn't reset the deadline or start a second timer.
- **Watchdog:** with the timer factory and exit function injected:
  - It is armed on every exit cause.
  - It fires once at T + 5.
  - It stays armed after `asyncio.run` returns; there is no cancel.
  - Tests never call the real `os._exit`.
- **Config:** a missing `shutdown.timeout_s`, 0, a negative value, NaN, infinity and 301 are all rejected; 30 loads.

## 6. Out of scope

- Cross-host or Postgres ownership and persistent account binding (WP 3.5).
- Acting on a kill: cancelling entries and flatten (1.5c).
- An unconditional bound on a native call that holds the GIL: it needs enforcement outside this interpreter (e.g. a service manager's stop timeout plus SIGKILL).
- **Item 8's "fail entry checks closed when required state is unavailable"** (deferred to 1.5d, before admission opens). Entries can't run until 1.5d, but `check()` doesn't yet fail closed on stale risk state:
  - It takes the daily-loss baseline from the last snapshot at or before the day start. If the snapshot loop was dead for days, that baseline is days old.
  - With no snapshot at all, it uses current equity, which makes the daily-loss check a no-op.
  - 1.5d must define a freshness bound for that baseline and reject entries when it is missing or stale, and while `risk.snapshot_overdue` is active.
- Wiring the runner, closing admission, startup recovery (1.5d).
- Windows support.

## 7. Documentation

- SPEC §4.11: add the single-writer lock and bounded shutdown to the cross-cutting rules.
- `config.yaml` and its schema: `shutdown.timeout_s`.
- The `core/runtime.py` module docstring: supervised loops, lock, shutdown.
- PLAN: a 1.5a done-note on the 1.5 row (append inside the last cell), and the repo-state line.
- The CLAUDE.md commands list: `run core` single-writer lock, and "Ctrl-C stops (twice = hard stop)".
