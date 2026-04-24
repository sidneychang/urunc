# Pragmatic review notes: view lock & containerd API

> Context: urunc is a low-level runtime component. For this layer, we should
> prefer simple/reliable primitives, avoid over-abstraction, and reuse mature
> ecosystem behavior where possible.

## 1) View lock implementation (`pkg/shiminject/inject.go`)

### Keep (already good for runtime layer)
- Keep file-lock + `flock` as the core primitive (cross-process, kernel-backed).
- Keep `users/` marker model for reference counting.
- Keep explicit cleanup-on-last-user path plus stale cleanup fallback.

### Lightweight improvements only
1. **Avoid over-engineering lock abstractions**
   - No need for complex lock frameworks/state machines in code.
   - A minimal helper like `withSharedViewLock(paths, func() error)` is enough if we want to reduce unlock/relock duplication.

2. **Add only essential observability**
   - Add 1-2 key log fields for lock contention (`shared_view_id`, wait time) instead of introducing heavy metrics pipeline changes.

3. **Stale cleanup trigger policy should stay cheap**
   - For runtime hot paths, avoid expensive periodic scanners by default.
   - Keep one-time startup cleanup + explicit last-user cleanup; only add periodic cleanup if production data proves necessity.

## 2) containerd API usage (`pkg/shiminject/inject.go`)

### Keep (already aligned with low-level runtime needs)
- Using generated service clients directly is acceptable in shim/runtime code.
- Current `grpcErr` normalization and metadata-based namespace/lease propagation are reasonable.

### Practical refinements (no big rewrites)
1. **Centralize context/header composition**
   - Keep low-level API, but add a tiny helper for `namespace + optional lease` to reduce repeated metadata wiring.

2. **Constrain wrapper surface area**
   - Do not build a large internal SDK around containerd APIs.
   - Only wrap repeated call sequences that already appear in multiple places (e.g., lease ensure + context attach).

3. **Prefer mature external behavior over custom GC logic**
   - Continue leveraging containerd lease/GC semantics for eventual reclaim.
   - Keep urunc-specific logic focused on local mount cleanup that containerd cannot perform.

4. **Configurability should be minimal and justified**
   - If making lease TTL configurable, keep one env/config knob with sane default.
   - Avoid exposing many tuning parameters that increase operational complexity.
