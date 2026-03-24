# Dynamic CRD Watch Registration & Removal

Controllers that depend on optional CRDs - think KEDA, Prometheus Operator, or anything a user *might* install - typically check CRD availability once at startup. Install the CRD later? Restart the controller. Remove it? The informer leaks, retrying list/watch forever.

This project shows how to handle both cases cleanly with controller-runtime, no restarts needed. The core logic lives in a reusable [`dynamicwatch`](pkg/dynamicwatch/) package that you can drop into your own controller.

If you want the system overview and implementation rationale, see [ARCHITECTURE.md](ARCHITECTURE.md) and [TECHNICAL_DESIGN.md](TECHNICAL_DESIGN.md).

## How it works

The setup is intentionally simple - three CRDs:

- **Widget** (always installed) - has optional `.spec.pluginRef` and `.spec.themeRef` fields
- **PluginConfig** (optional) - provides a `setting` value
- **Theme** (optional) - provides a `colorScheme` value

A single controller reconciles Widgets. When a ref is set and the corresponding CRD exists, it reads the referenced resource and reports the condition as `True`. When the CRD is absent - `False`, reason `CRDNotAvailable`.

```
Startup:
  Widget CRD exists          → watch via builder.For()
  PluginConfig CRD missing   → skip, no watch registered

Runtime - CRD installed:
  CRD watch fires            → Ensure() → ctrl.Watch(source.Kind(...))

Runtime - CRD removed:
  CRD watch fires            → cache.RemoveInformer()
                              → reset state, requeue affected objects
```

## Using `dynamicwatch` in your controller

The package provides a generic `Watcher[T]` that handles one optional CRD's lifecycle. Need multiple optional CRDs? Create one watcher per CRD.

### Setup

Wire the watcher in `SetupWithManager` using a fluent builder that mirrors controller-runtime's API:

```go
func (r *MyReconciler) SetupWithManager(mgr ctrl.Manager) error {
    // 1. Build the watcher - GVK is derived from the scheme automatically
    r.optionalWatch, err = dynamicwatch.For[*v1alpha1.OptionalResource](mgr, "optionalresources.example.com").
        WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(r.mapOptionalToParent)).
        EnqueueOnCRDChange(r.allAffectedParents).
        Build()

    // 2. Wire it up - Watcher implements source.SyncingSource
    return ctrl.NewControllerManagedBy(mgr).
        For(&v1alpha1.MyResource{}).
        Named("my-controller").
        WatchesRawSource(r.optionalWatch).
        Complete(r)
}
```

The two callbacks answer different questions:

- **`WithEventHandler`** - "an OptionalResource changed, which parent objects need to reconcile?" Typically a field index lookup on the parent's ref field:

```go
// Maps an OptionalResource event to the parent objects that reference it.
func (r *MyReconciler) mapOptionalToParent(ctx context.Context, obj *v1alpha1.OptionalResource) []reconcile.Request {
    var parents v1alpha1.MyResourceList
    _ = r.List(ctx, &parents, client.MatchingFields{"spec.optionalRef": obj.GetName()})

    requests := make([]reconcile.Request, 0, len(parents.Items))
    for i := range parents.Items {
        requests = append(requests, reconcile.Request{
            NamespacedName: client.ObjectKeyFromObject(&parents.Items[i]),
        })
    }
    return requests
}
```

- **`EnqueueOnCRDChange`** - "the CRD itself appeared or disappeared, which parent objects should re-evaluate?" Usually all parents that could reference this optional type:

```go
// Returns all parent objects that have a ref set, so they can
// re-evaluate their condition after a CRD lifecycle change.
func (r *MyReconciler) allAffectedParents(ctx context.Context) []reconcile.Request {
    var parents v1alpha1.MyResourceList
    _ = r.List(ctx, &parents, client.MatchingFields{"has-optionalRef": "true"})

    requests := make([]reconcile.Request, 0, len(parents.Items))
    for i := range parents.Items {
        requests = append(requests, reconcile.Request{
            NamespacedName: client.ObjectKeyFromObject(&parents.Items[i]),
        })
    }
    return requests
}
```

For owned resources (the dynamic equivalent of `builder.Owns()`), use `EnqueueForOwner` instead of `WithEventHandler` - it uses owner references so no mapper is needed:

```go
r.ownedWatch, err = dynamicwatch.For[*v1alpha1.OwnedResource](mgr, "ownedresources.example.com").
    EnqueueForOwner(&v1alpha1.MyResource{}, handler.OnlyControllerOwner()).
    EnqueueOnCRDChange(r.allAffectedParents).
    Build()
```

The Watcher implements `source.SyncingSource`, so it plugs directly into the builder via `WatchesRawSource`. The controller framework calls `Start` and `WaitForSync` automatically - no manual wiring needed.

The builder also supports a few optional knobs:

- `WithSyncTimeout(d)` - how long to wait for the informer to sync before tearing down and retrying (default: 30s)
- `WithEventRecorder(recorder)` - emit Kubernetes events on watch lifecycle transitions (activated, deactivated, sync failed)
- `WithNamespaces(...)` - restrict the private object cache to specific namespaces (important for namespace-scoped RBAC)
- `WithCRDCache(...)` - share a single CRD informer across multiple watchers

### Reconcile loop

In your `Reconcile` method, use `TryGet` to check CRD availability and read the object in one call:

```go
func (r *MyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // ...

    optional := &v1alpha1.OptionalResource{}
    available, err := r.optionalWatch.TryGet(ctx, key, optional)
    if err != nil {
        if client.IgnoreNotFound(err) == nil {
            return ctrl.Result{}, r.setNotFoundCondition(ctx, obj)
        }
        return ctrl.Result{}, err
    }
    if !available {
        // CRD not installed or informer still syncing.
        // The watcher will requeue affected objects automatically
        // once the cache is ready.
        return ctrl.Result{}, r.setUnavailableCondition(ctx, obj)
    }

    // Use optional...
}
```

`TryGet` calls `Ensure` internally and absorbs cache invalidation races - you never need to handle them yourself. The return values are:

- `(true, nil)` - object found
- `(true, err)` - CRD available but cache error (e.g. `NotFound`)
- `(false, nil)` - CRD unavailable or informer removed mid-read

There's also `TryList` with the same semantics for listing objects.

### Observability

The watcher exposes its state through three channels:

**`Status()`** returns a `WatcherStatus` with `Available` and `Reason` fields. Useful for health checks or debug endpoints without parsing logs:

```go
status := r.optionalWatch.Status()
// status.Reason is one of: Ready, CRDNotFound, Syncing, Pending, NotStarted
```

**Prometheus metrics** are registered automatically with controller-runtime's metrics registry:

- `dynamicwatch_active{crd="..."}` - gauge, 1 when the watch is active, 0 otherwise
- `dynamicwatch_state_transitions_total{crd="...", transition="..."}` - counter tracking `activated`, `deactivated`, `invalidated`, and `sync_failed` transitions

**Kubernetes events** are opt-in via `WithEventRecorder`. When enabled, the watcher emits events on the CRD object:

- Normal `WatchActivated` - informer synced, watch ready
- Warning `WatchDeactivated` - CRD removed, watch torn down
- Warning `WatchSyncFailed` - informer sync failed, will retry

### Prerequisites

None on the manager side. Each Watcher creates its own private cache with `ReaderFailOnMissingInformer: true` internally - you don't need to configure anything special on your manager cache.

## Things that will bite you

When developing this PoC I learned the following:

### `ReaderFailOnMissingInformer` is critical (but handled for you)

Without `ReaderFailOnMissingInformer: true` on a cache, a `Get()` call after `RemoveInformer` silently creates a *new* informer for the removed GVK. That informer tries to list/watch against a non-existent API and blocks on `WaitForCacheSync` forever. Your controller is now a very expensive no-op.

The `dynamicwatch` package sets this flag on each Watcher's private cache automatically. If you're building something similar from scratch, don't forget it.

### CRD deletion generates spurious events

During CRD deletion, status update events arrive *before* `DeletionTimestamp` is set. If you only check `DeletionTimestamp`, the controller re-registers the watch mid-deletion - creating a race that deadlocks the worker goroutine.

Check both:

```go
crdRemoved := !crd.DeletionTimestamp.IsZero() || !isCRDEstablished(crd)
```

### The `RemoveInformer` / read race (handled for you)

If `RemoveInformer` fires between `Ensure()` returning ready and the cache read, the cache returns `ErrResourceNotCached`. The Watcher catches this internally - `TryGet`/`TryList` return `(false, nil)`, indistinguishable from "CRD not installed". The watcher resets its state, and recovery then depends on the next CRD event or another enqueue path. No special handling is needed in your reconciler.

## Quick start

The fastest way to see it in action:

```bash
make kind-create
make deploy
kubectl rollout status deployment/dynamic-watch-poc-controller-manager \
  -n dynamic-watch-poc-system --timeout=60s

# Create a Widget that references a plugin - CRD doesn't exist yet
kubectl apply -f - <<EOF
apiVersion: demo.example.com/v1alpha1
kind: Widget
metadata:
  name: test-widget
spec:
  pluginRef: my-plugin
EOF

kubectl get widget test-widget -o jsonpath='{.status.conditions}' | jq .
# → PluginReady: False, reason: PluginCRDNotAvailable

# Install the CRD at runtime - no restart
kubectl apply -f config/crd/bases/demo.example.com_pluginconfigs.yaml

# Create the PluginConfig it's looking for
kubectl apply -f - <<EOF
apiVersion: demo.example.com/v1alpha1
kind: PluginConfig
metadata:
  name: my-plugin
spec:
  setting: "hello"
EOF

kubectl get widget test-widget -o jsonpath='{.status.conditions}' | jq .
# → PluginReady: True, reason: PluginApplied

# Pull the rug - remove the CRD entirely
kubectl delete crd pluginconfigs.demo.example.com

kubectl get widget test-widget -o jsonpath='{.status.conditions}' | jq .
# → PluginReady: False, reason: PluginCRDNotAvailable
```

> [!NOTE]
> See [DEV.md](DEV.md) for build options, testing modes, and cluster setup.

## References

- [controller-runtime PR #2285](https://github.com/kubernetes-sigs/controller-runtime/pull/2285) - `cache.RemoveInformer()`, originated from Gatekeeper's fork
- [controller-runtime Issue #540](https://github.com/kubernetes-sigs/controller-runtime/issues/540) - the original dynamic watch discussion
- [Crossplane Realtime Compositions](https://github.com/crossplane/crossplane/blob/main/internal/controller/apiextensions/definition/reconciler.go) - the most sophisticated dynamic watch implementation out there
- [Gatekeeper Dynamic Cache](https://github.com/open-policy-agent/gatekeeper/tree/master/third_party/sigs.k8s.io/controller-runtime/pkg/dynamiccache) - the fork that inspired upstream support
