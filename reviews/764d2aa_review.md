# Code Review: 764d2aa `feat(view): use snapshot views for boot artifacts`

## Context reviewed

- Contributor guide: `CONTRIBUTING.md` and `docs/developer-guide/contribute.md`
- Project overview: `README.md`
- Commit diff: `764d2aa`

## Review summary (implementation + code style)

### Implementation clarifications from follow-up discussion

1. **Lease cleanup model is explicit-delete, not lease-expiration-driven (current code)**
   - The shared-view lease is created by ID via `leases.Create(...)` with no expiry metadata.
   - Lease deletion is done best-effort after snapshot removal in cleanup path.
   - So the current implementation does **not** rely on automatic lease timeout/expiration to recover orphaned views.

   **Suggestion:** if we want restart/crash resilience without extra state files, we should explicitly design an expiration policy and verify containerd GC behavior for that policy.

2. **Absolute-path handling in `bindViewFilesToMonRootfs()` depends on path contract**
   - If our annotation contract guarantees relative paths only, current `filepath.Join(viewMountPath, artifactPath)` is acceptable and no extra defense is needed.
   - If absolute paths are possible, `filepath.Join` will ignore `viewMountPath` and can mis-resolve source files.

   **Current repo signal:** docs/examples show absolute-style paths like `/unikernel/kernel` and `/unikernel/initrd`, so this contract should be clarified in code/docs/tests.

### Important implementation risks (non-blocking but recommended)

3. **View lifecycle bookkeeping is memory-only in shim wrapper**
   - `wrapper.views` is in-memory only and populated after successful `Create`.
   - `Delete` cleanup depends on this map.
   - On shim restart/crash, in-memory state is lost; later `Delete` cannot look up view info, so user marker/snapshot view cleanup can be skipped.

   **Suggested improvement:** either (a) adopt an explicit lease-expiration policy, or (b) add a durable marker/reconstruction path from annotations/state on `Delete`.

4. **Config rewrite does not preserve file mode**
   - `injectViewPathToConfig()` writes `config.json` with a fixed `0600` mode.
   - This is functionally acceptable for root-owned bundle paths, but can unexpectedly tighten permissions compared to original file mode.

   **Suggested improvement:** preserve existing mode (`os.Stat` + `os.WriteFile` with original perm), or document why forced `0600` is required.

### Code style / readability suggestions

5. **Unnecessary temporary error variable in `chooseRootfs()`**
   - `retErr` is introduced but only used for immediate return paths.
   - This pattern adds noise and diverges from idiomatic Go short error handling.

   **Suggested refactor:** return directly from `switchMonRootfs()` and direct `fmt.Errorf(...)` branches without `retErr`.

6. **Logging style consistency between packages**
   - `shiminject` logger includes subsystem field, while `shimtask` uses `logrus.StandardLogger()` directly.
   - Mixed logging styles reduce filtering consistency for operators.

   **Suggested improvement:** use a consistent subsystem-scoped logger in `shimtask` too.

## Positive notes

- Shared-view creation path includes takeover/wait logic and lock-based serialization.
- Error handling is generally defensive (best-effort cleanup on `Create` failure and config injection failure).
- Rootfs-selection logic keeps the new snapshot-view state constrained to rootfs choice, minimizing blast radius.
