# Runtime-focused quality review (follow-up)

## Scope
- `pkg/shiminject/inject.go`
- Focus: low-level runtime reliability, correctness under long-running workloads, and operational safety.

## High-priority points

1. **Lease TTL is not refreshed on reuse**
   - Current flow sets `containerd.io/gc.expire` only on lease creation.
   - If lease already exists (`AlreadyExists`), TTL is not extended.
   - Risk: long-lived containers sharing a view may outlive the initial TTL window.

   **Suggestion (minimal):** on `AlreadyExists`, issue a lightweight lease update to refresh `gc.expire`.

2. **One-time stale cleanup may be insufficient for very long-lived shim processes**
   - `sync.Once` cleanup runs only on first create path.
   - Later crash leftovers (after startup) are not revisited by this mechanism.

   **Suggestion (runtime-friendly):** keep one-time cleanup as default, but add an optional low-frequency recheck gate (e.g. interval-based, disabled by default).

3. **`waitForSharedViewReady` timeout is fixed and short (5s)**
   - Under slower storage/snapshotters, fixed timeout may cause avoidable create failures.

   **Suggestion:** make timeout configurable with a conservative default; keep current fast path unchanged.

## Medium-priority points

4. **Hardcoded runtime paths and TTL values**
   - `sharedViewsRoot`, `sharedViewLeaseTTL`, and lock behavior are currently compile-time constants.

   **Suggestion:** expose only minimal knobs (env/config) needed by operators, with strict defaults.

5. **Structured logging context can improve operability**
   - Some warnings do not always carry key identifiers (`shared_view_id`, `snapshotter`, `lease_id`).

   **Suggestion:** add consistent fields in warning/error paths for faster incident triage.

## Keep as-is (good runtime choices)

- Kernel-backed `flock` for cross-process synchronization.
- Marker-file reference model (`users/`) + explicit last-user cleanup path.
- Local stale mount cleanup kept separate from containerd-side GC responsibilities.
