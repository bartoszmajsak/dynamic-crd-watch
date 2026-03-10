# Dynamic CRD Watch Registration & Removal

Controllers that depend on optional CRDs - think KEDA, Prometheus Operator, or anything a user *might* install - typically check CRD availability once at startup. Install the CRD later? Restart the controller. Remove it? The informer leaks, retrying list/watch forever.

This project shows how to handle both cases cleanly with controller-runtime, no restarts needed. The core logic lives in a reusable [`dynamicwatch`](pkg/dynamicwatch/) package that you can drop into your own controller.

## How it works

The setup is intentionally simple - two CRDs:

- **Widget** (always installed) - has an optional `.spec.pluginRef` field
- **PluginConfig** (optional) - may or may not exist at any point

A single controller reconciles Widgets. When `pluginRef` is set and the PluginConfig CRD exists, it reads the referenced PluginConfig and reports `PluginReady: True`. When the CRD is absent - `PluginReady: False`, reason `PluginCRDNotAvailable`.

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
        EnqueueOnObjectChange(r.mapOptionalToParent).   // when an OptionalResource changes
        EnqueueOnCRDChange(r.allAffectedParents).        // when the CRD itself appears/disappears
        Build()

    b := ctrl.NewControllerManagedBy(mgr).
        For(&v1alpha1.MyResource{}).
        Named("my-controller")

    // 2. Register - adds a CRD watch to detect install/removal
    r.optionalWatch.Register(b)

    // 3. Build + Bind - connects the watcher to the controller
    c, err := b.Build(r)
    r.optionalWatch.Bind(c)

    return nil
}
```

The three-step `Build` / `Register` / `Bind` dance exists because controller-runtime's builder doesn't expose the `controller.Controller` until after `Build()`, but the CRD watch must be registered *before* `Build()`. There's no way around this without wrapping the builder.

### Reconcile loop

In your `Reconcile` method, call `Ensure` to check availability and register the watch if the CRD just appeared:

```go
func (r *MyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // ...

    switch r.optionalWatch.Ensure(ctx) {
    case dynamicwatch.Unavailable:
        // CRD not installed - set a condition and move on.
        return ctrl.Result{}, r.setUnavailableCondition(ctx, obj)
    case dynamicwatch.Syncing:
        // Watch registered, informer cache catching up. Short requeue to
        // let the initial LIST complete - the next reconcile checks HasSynced().
        return ctrl.Result{RequeueAfter: 200 * time.Millisecond}, nil
    case dynamicwatch.Ready:
        // Cache is synced, proceed to read.
    }

    // Read the optional resource through the watcher's cache-aware Get.
    optional := &v1alpha1.OptionalResource{}
    if err := r.optionalWatch.Get(ctx, key, optional); err != nil {
        if errors.Is(err, dynamicwatch.ErrCacheInvalidated) {
            // Informer was removed between Ensure and Get (race).
            // Watcher already reset its state - just requeue.
            return ctrl.Result{RequeueAfter: time.Second}, nil
        }
        return ctrl.Result{}, err
    }

    // Use optional...
}
```

### Prerequisites

Your cache **must** have `ReaderFailOnMissingInformer: true`:

```go
mgr, err := ctrl.NewManager(cfg, ctrl.Options{
    Cache: cache.Options{
        ReaderFailOnMissingInformer: true,
    },
})
```

Without it, a `Get` after informer removal silently creates a new informer that blocks forever. See ["Things that will bite you"](#readerfailonmissinginformer-is-not-optional) below.

## Things that will bite you

When developing this PoC I learned the following:

### `ReaderFailOnMissingInformer` is not optional

Without `ReaderFailOnMissingInformer: true` on the cache, a `r.Get()` call after `RemoveInformer` silently creates a *new* informer for the removed GVK. That informer tries to list/watch against a non-existent API and blocks on `WaitForCacheSync` forever. Your controller is now a very expensive no-op.

### CRD deletion generates spurious events

During CRD deletion, status update events arrive *before* `DeletionTimestamp` is set. If you only check `DeletionTimestamp`, the controller re-registers the watch mid-deletion - creating a race that deadlocks the worker goroutine.

Check both:

```go
crdRemoved := !crd.DeletionTimestamp.IsZero() || !isCRDEstablished(crd)
```

### The `RemoveInformer` / `r.Get` race

If `RemoveInformer` fires between `ensurePluginWatch()` returning `true` and `r.Get(PluginConfig)`, the cache returns `ErrResourceNotCached`. You must catch this, reset the watch flag, and requeue. Miss it and you're back to a deadlocked controller.

Otherwise, your controller can get deadlocked.

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
