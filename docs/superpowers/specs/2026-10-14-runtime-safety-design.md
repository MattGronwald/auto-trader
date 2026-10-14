# Runtime safety design (WP 1.5a)

Date: 2026-10-14. Status: approved in conversation (sections 1-3), pending written-spec review.
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
- Only one `run core` process can hold a given journal. A second one waits a bounded time, then fails startup with the holder's PID.
- Kill/control polling, feed ticks and equity snapshots run as independent supervised loops. A hung step in one never delays the others. Stalls are logged, including `risk.snapshot_overdue` when the last snapshot is older than 2 x its interval.
- A first SIGINT/SIGTERM shuts down within `shutdown.timeout_s`. A second signal stops at once. A watchdog thread ends the process if even that hangs.
- No behaviour change for entries, exits or the feed's data handling.

Owner decisions taken while designing (2026-10-14, recorded in PLAN 1.5):
- The lock file is **derived from the database path**, with no new config key.
- Shutdown uses a **graceful budget, a second-signal hard stop and a watchdog thread**.

## 2. Single-writer lock (`core/lock.py`)

- **Path:** derived from `database.url`. SQLite `sqlite+aiosqlite:///data/autotrader.db` gives `data/autotrader.db.lock`, beside the journal file. It is derived from the URL as `config.py` has already resolved it: `_resolve_sqlite_url` makes a relative SQLite path absolute against `config.yaml`'s directory. Two configs pointing at the same journal therefore always contend for the same lock.
- **Non-file databases:** any URL that is not a file-backed SQLite database fails startup with `single-writer lock needs a file journal; cross-host ownership is WP 3.5`. That includes Postgres and in-memory SQLite.
- **Mechanism:** an exclusive, non-blocking `fcntl.flock` on the lock file, which is opened or created.
  - The file descriptor stays open for the process's lifetime. The OS releases the lock when the process exits, including a crash, `kill -9` or `os._exit`. Releasing it explicitly at the end of shutdown means closing the descriptor.
  - After acquiring the lock, the file is truncated and the PID and the start time (UTC ISO) are written. These are diagnostic only; the lock is what excludes other processes.
- **When:** first in `run_core`, before anything opens or writes the journal (`register_strategy`, backfill, the bus).
- **Contention:**
  - A second core retries the non-blocking lock once per second for `shutdown.timeout_s + 5` seconds. That way a restart can begin while the previous process is still shutting down: the lock is released only once the old process has exited, so its SQLite work has been committed or rolled back.
  - On timeout, startup fails with `journal locked by PID <pid> since <ts>` and exit code 1.
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
| `risk.snapshot` | `risk.snapshot_equity()` | 60 s | 120 s, logged as `risk.snapshot_overdue` | warning |

- The `control.poll` loop keeps the latest `ControlStatus` so that 1.5c can act on kill changes without a second poller. In 1.5a, a kill is only logged, as today.
- The intervals and thresholds are constants in `runtime.py`, with no new config keys apart from `shutdown.timeout_s`.
- The entry fail-closed behaviour is unchanged: `ControlGate` already fails closed, and no entries run until 1.5d.
- The main loop becomes "supervise until stop or stream death". It raises on stream death and runs the stall check about once a second.

## 4. Bounded shutdown, second signal, watchdog

**Config:** `shutdown.timeout_s` is required in the schema: a finite number with `0 < v <= 300`. `config.yaml` sets **30**, which is above the Cycle Runner's 20 s close budget (`CLOSE_BUDGET_S`) wired in 1.5d. It is infrastructure config; the profile hash is unchanged.

**First SIGINT/SIGTERM (graceful):**
1. Set the stop event. The CLI starts the watchdog.
2. Admission: nothing to close in 1.5a. 1.5d inserts "close admission, then `runner.close()`" here.
3. `source.stop()`, then `supervisor.stop(deadline)`.
4. `bus.close()`, bounded by the remaining time.
5. `engine.dispose()`, then release the lock.

- **Deadline:** the whole sequence runs under one deadline, set to the time the signal was received plus `shutdown.timeout_s`.
  - Whatever is unfinished at the deadline is cancelled and logged as `core.shutdown_forced`, naming the step and the loops that were cancelled.
  - Abandoned work (open cycles, `pending`/`unknown` orders) stays in the journal for 1.5d's startup recovery and is logged as `core.shutdown_abandoned`. Forced cancellation does not prove that broker work stopped.
- **Exit code:** 0 when clean, 1 when the deadline forced cancellation.

**Second signal (hard):** skip the rest of the drain, cancel all tasks at once, dispose the engine and release the lock without waiting. Exit code **130**. This applies at any point after the first signal.

**Watchdog:**
- On the first signal, the CLI starts a daemon `threading.Timer` for `shutdown.timeout_s + 5` s. The thread is needed because a timer on the event loop can't fire while the loop is blocked in synchronous code.
- If the timer fires, it writes `core.watchdog_exit` to stderr directly (not through the event loop) and calls `os._exit(70)`. Skipping cleanup is acceptable: SQLite rolls back uncommitted transactions, and the OS releases the `flock`.
- A process that exits cleanly first cancels the timer.

| Exit code | Meaning |
|---|---|
| 0 | clean stop |
| 1 | startup failure (including lock timeout), stream death, or a forced shutdown deadline |
| 130 | second signal |
| 70 | watchdog |

## 5. Testing

- **Lock:**
  - A second acquisition from a **subprocess** times out with the holder's PID in the error. A short wait is injected; no long real sleeps.
  - Acquisition succeeds after the holder exits.
  - The derived path for a SQLite URL is correct.
  - Postgres, `:memory:` and other non-file URLs fail with the documented error.
  - The lock is released on every shutdown path: clean, forced and second signal.
- **Supervisor**, driven by an injected clock and events, never by wall-clock assertions (lesson from PR #13):
  - Stall and recovery logging happens once per transition.
  - `risk.snapshot_overdue` is logged at 2 x the interval.
  - A raising `step()` keeps its loop running.
  - A dead task is restarted.
  - `stop(deadline)` cancels the steps that are still hanging and returns their names.
- **Independence:** a fake bar source whose REST `latest()` hangs forever. A kill-file change is still logged by `control.poll`, and `feed.tick.stalled` is logged.
- **Shutdown:**
  - A hanging `bus.close()` or loop step is cut off at the deadline: exit 1, and `core.shutdown_forced` is logged.
  - A second signal skips the drain and exits with 130.
  - Stream death still fails `run_core`.
- **Watchdog:** with the timer factory and exit function injected, it fires once after the deadline and not after a clean exit. Tests never call the real `os._exit`.
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
