# Technical Design

## Purpose

`ARCHITECTURE.md` explains the system shape. This document explains why `dynamicwatch` is implemented the way it is, which invariants it relies on, and where the design is intentionally limited.

This is for:

- contributors changing `pkg/dynamicwatch/`
- controller authors deciding whether this pattern fits their operator

## Goals

- Handle optional CRDs appearing and disappearing at runtime without restarting the controller.
- Keep consumer reconcile logic simple: "is the optional type usable right now, and if so can I read it?"
- Avoid cross-controller interference when multiple controllers in one manager watch the same optional GVK.
- Stay inside controller-runtime primitives instead of building a bespoke informer stack or polling discovery.

## Non-Goals

- Discovering arbitrary CRDs at runtime without a compiled Go type and scheme registration.
- Hiding every transient unavailable state during CRD install/remove churn.
- Handling CRD schema compatibility or served-version changes beyond "is this CRD currently watchable?"
- Eliminating the extra caches required for informer isolation.

## Public Contract

### Builder

`For[T](mgr, crdName)` starts a fluent builder for one optional CRD. The CRD name must be in `<plural>.<group>` format - `Build()` validates this and fails fast on obviously wrong names (bare plurals, empty strings, missing group).

`Build()` requires:

- exactly one event mapping strategy: `WithEventHandler(...)` or `EnqueueForOwner(...)`
- `EnqueueOnCRDChange(...)`, because the library needs a way to requeue affected parents when the CRD appears or disappears

Important builder options:

- `WithCRDCache(...)` shares one CRD informer across multiple watchers
- `WithPredicates(...)` filters events for the watched object type
- `WithNamespaces(...)` scopes the private object cache to specific namespaces
- `WithSyncTimeout(d)` overrides the 30s default for informer sync (see "Sync Promotion" rationale)
- `WithEventRecorder(recorder)` enables Kubernetes event emission on lifecycle transitions

### Watcher

`Watcher[T]` implements `source.SyncingSource`, so it plugs directly into `WatchesRawSource(...)`.

The consumer-facing methods are:

| Method | Meaning |
|---|---|
| `Ensure(ctx) bool` | Make sure the dynamic watch is registered and report whether the object cache is ready for reads. |
| `TryGet(ctx, key, obj)` | Preferred read path. Returns `(false, nil)` when the optional type is not usable right now. |
| `TryList(ctx, list, opts...)` | Same semantics as `TryGet`, but for lists. |
| `Status()` | Returns a `WatcherStatus` with `Available` bool and `Reason` string. Designed for health checks and debug endpoints. |
| `Available()` | Point-in-time diagnostic snapshot only. Equivalent to `Status().Available`. |

`WaitForSync()` only waits for the CRD informer, not the optional object's informer. That means controller startup can complete while a dynamic object watch is still absent or still syncing. Consumers must treat `(false, nil)` from `TryGet` and `TryList` as a normal state.

## Core Components

| Component | Responsibility |
|---|---|
| `Watcher[T]` | Tracks one optional CRD and manages the informer lifecycle for `T`. |
| Private object cache | Holds the informer for `T`; isolated per watcher. |
| `CRDCache` | Watches CRDs so the watcher can react to install/remove events. |
| `RequeueParentsFn` | Returns reconcile requests for parents affected by CRD lifecycle changes. |
| `waitForSyncAndRequeue` | Promotes a watch to ready once the informer syncs, then requeues parents. |

## Internal State Model

The watcher keeps a small mutex-protected state machine:

| Field | Meaning |
|---|---|
| `crdExists` | The CRD is currently known to be present and usable enough to attempt a watch. |
| `watching` | The object source has been started, but may not have synced yet. |
| `active` | The object informer has synced and reads are safe. |
| `generation` | Monotonic counter used to prevent stale sync goroutines from promoting old state. |
| `cancelSyncWaiter` | Cancel function for the current sync waiter, if one exists. |

Two details matter:

- `watching` and `active` are intentionally different states. A watch can be registered but not yet ready.
- `started` is an `atomic.Bool` (not mutex-guarded) because `Status()` reads it concurrently for health checks. `ctx` and `queue` are populated by `Start()` and treated as controller lifecycle state, not per-reconcile state.

## Design Rationale

### Private Object Cache Per Watcher

Each watcher creates its own `cache.Cache` for `T` instead of using the manager's shared cache.

That isolation is the core safety property of the design. If two controllers in the same manager both watched the same optional GVK through a shared cache, one controller calling `RemoveInformer()` would tear the informer down for both. A private cache keeps informer removal local to the watcher that owns it.

The private cache is also configured with `ReaderFailOnMissingInformer: true`. Without that flag, a `Get()` or `List()` after `RemoveInformer()` silently spins up a fresh informer for a resource whose API is disappearing - see the README's "ReaderFailOnMissingInformer is critical" section for the full horror story.

With the flag enabled, the cache fails loudly with `*cache.ErrResourceNotCached`, which the watcher converts into `(false, nil)` for consumers.

### Separate CRD Cache From Object Cache

CRD lifecycle and optional-object reads are handled by different caches on purpose.

The CRD side can be configured in two ways:

- shared `CRDCache`: one cache watches all CRDs, and each watcher filters client-side by name predicate
- dedicated per-watcher CRD cache: created when `WithCRDCache(...)` is omitted, filtered server-side on `metadata.name`

The shared form reduces the number of LIST/WATCH connections for clusters with many watchers. The dedicated form reduces baseline CRD traffic when only one or two watchers exist.

### Why The Object Cache Does Not Use `ByObject` For `T`

The watched type `T` may refer to a CRD that is not installed yet. `cache.New(..., cache.Options{ByObject: ...})` needs the REST mapper to resolve the type at cache construction time, which fails if the CRD does not exist yet.

Because of that, the object cache is created without `ByObject` scoping for `T`. Namespace scoping is instead handled through `DefaultNamespaces`, which is what `WithNamespaces(...)` configures.

### CRD Removal Detection Uses More Than `DeletionTimestamp`

The watcher treats either of these as "do not watch":

- `DeletionTimestamp` is set
- `Established` is not `True`

That rule is slightly conservative, but it avoids re-registering informers against a CRD that is in the middle of going away. The README's "CRD deletion generates spurious events" section covers the full pitfall - in short, Kubernetes can emit `Established=False` status updates before `DeletionTimestamp` is set, and only checking the timestamp causes a race that deadlocks the worker goroutine.

### Sync Promotion Must Be Cancellable And Generation-Safe

`Ensure()` starts the object source and then launches a goroutine that waits for informer sync. That goroutine is deliberately guarded in two ways:

- the timeout context is created in `Ensure()` and stored through `cancelSyncWaiter`, so `onCRDChange()` can cancel an in-flight waiter immediately
- the watcher's `generation` is captured when the goroutine starts, and checked again before promoting `active`

Without those guards, a stale sync waiter could keep blocking after the CRD was removed, or worse, promote a watcher back to ready after teardown.

### Read-Side TOCTOU Is Collapsed Into A Simple API

There is an unavoidable race between:

1. `Ensure()` returning ready
2. a concurrent CRD teardown removing the informer
3. the subsequent cache read

When that happens, the cache returns `*cache.ErrResourceNotCached`. The watcher treats that as an internal invalidation event:

- clear `watching`, `active`, and `crdExists`
- increment `generation`
- cancel any in-flight sync waiter

Consumers do not see this as a special error. `TryGet()` and `TryList()` return `(false, nil)`, the same as "CRD not currently usable." That API is intentionally less expressive than the internal state, because it keeps reconcile code simple and pushes race handling into the library.

### Requeue Semantics Are Targeted, Not Universal

The library does requeue affected parents automatically, but only on specific transitions:

- when the CRD first appears
- when an active watch is torn down because the CRD is being removed
- when a newly started informer finishes syncing
- when informer sync fails and the watcher cleans the informer up

One thing it does **not** do: `handleCacheInvalidated()` does not directly call `requeueAll()`. After a read-side race, recovery depends on a later CRD event, a later sync completion/failure path, or another normal enqueue path in the controller.

That trade-off keeps the read path cheap and avoids duplicating requeue storms, but it also means the docs should not claim that every invalidation causes an immediate parent requeue.

### CRD Objects Are Trimmed Before Caching

The shared CRD cache may watch all CRDs in the cluster, so memory overhead matters. `transformCRDForCache(...)` strips fields that the watcher does not need:

- `spec`
- `managedFields`
- `annotations`

The watcher only needs metadata and status, especially the CRD name, `DeletionTimestamp`, and `Established` condition.

## Known Limitations And Trade-Offs

### For consumers

- The watched type must exist in the scheme at build time. This pattern does not support arbitrary unstructured CRDs discovered only at runtime.
- `Build()` validates CRD name format (`<plural>.<group>`), but a well-formed typo (e.g. `pluginconfig.demo.example.com` instead of `pluginconfigs.demo.example.com`) passes validation. The watcher will simply remain unavailable forever at runtime.
- Controller `WaitForSync()` does not imply the optional object cache is ready. Early reconciles can legitimately see `(false, nil)` until the dynamic informer syncs.
- `TryGet()` and `TryList()` intentionally collapse multiple causes into the same `(false, nil)` result: CRD absent, CRD deleting, informer syncing, or informer invalidated mid-read. That keeps consumers simple, but it reduces diagnostic precision. For deeper insight, `Status()` returns a `Reason` that distinguishes between these states.
- Correctness depends on the caller's `EnqueueOnCRDChange(...)` and mapping functions being complete. If those functions miss a parent, the library cannot invent the missing reconcile request.
- `EnqueueRequestsFromMapFunc` cannot return errors. In the example controller, a failed `List()` during mapping or CRD-change requeue is logged and the reconcile requests are dropped until another event arrives.
- A shared `CRDCache` watches all CRDs cluster-wide. That lowers connection fanout, but it increases baseline watch traffic and requires cluster-scoped `list/watch` permission on CRDs.
- Without `WithNamespaces(...)`, the private object cache watches all namespaces. In a namespace-scoped deployment this can create RBAC-driven sync failures that look like permanent unavailability.
- The design reasons about CRD lifecycle, not schema evolution. If a CRD stays present but changes shape or served versions incompatibly, the watcher does not provide special handling beyond whatever the client cache and scheme already do.

### Implementation constraints

- Every watcher adds a private cache. That is the cost of informer isolation.
- Informer sync timeout defaults to 30 seconds, configurable via `WithSyncTimeout(d)`. Repeated sync failure can cause repeated teardown and requeue cycles.
- Temporary status flapping is expected during rapid install/remove/reinstall cycles. The library chooses eventual correctness over hiding every transient state.

## Version Constraints

This design depends on `cache.Options.ReaderFailOnMissingInformer` and `cache.ErrResourceNotCached`, both added in controller-runtime v0.20.0. This repo uses v0.23.1. See `MIGRATION.md` for the full dependency background.

## Testing Strategy

The design is validated at three layers:

| Layer | What it checks |
|---|---|
| Unit tests | Builder validation, watcher state transitions, invalidation handling, sync waiter behavior, and concurrency-sensitive paths. |
| Envtest | In-process manager against an embedded API server. |
| Integration / e2e | Real-cluster CRD lifecycle, deployed manifests, RBAC, and controller behavior under actual install/remove flows. |

Important testing choices:

- CRD install/remove operations use an uncached client, because the manager's main cache is not the source of truth for the watcher's auxiliary CRD caches.
- Concurrency-heavy tests are run with the race detector.
- The most valuable lifecycle scenarios are install, remove, reinstall, and rapid CRD churn while reads are happening.

## File Guide

- `pkg/dynamicwatch/watcher.go`: watcher lifecycle, CRD handling, sync waiter, cache invalidation.
- `pkg/dynamicwatch/builder.go`: builder contract and cache construction.
- `pkg/dynamicwatch/metrics.go`: Prometheus metric definitions (`dynamicwatch_active`, `dynamicwatch_state_transitions_total`).
- `pkg/dynamicwatch/watcher_test.go`: unit tests for lifecycle and race handling.
- `internal/controller/widget_controller.go`: example consumer with two independent optional CRDs.
- `internal/controller/widget_controller_int_test.go`: envtest integration tests for the example controller.
- `internal/controller/dynamicwatch_int_test.go`: envtest integration tests for dynamicwatch lifecycle behavior.

## Related Docs

- `README.md`: adoption-oriented usage guide
- `ARCHITECTURE.md`: high-level system view
- `DEV.md`: running and testing the project
- `MIGRATION.md`: controller-runtime dependency notes
