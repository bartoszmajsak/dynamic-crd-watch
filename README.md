# Dynamic CRD Watch Registration & Removal

Controllers that depend on optional CRDs -think KEDA, Prometheus Operator, or anything a user *might* install -typically check CRD availability once at startup. Install the CRD later? Restart the controller. Remove it? The informer leaks, retrying list/watch forever.

This project shows how to handle both cases cleanly with controller-runtime, no restarts needed.

## How it works

The setup is intentionally simple -two CRDs:

- **Widget** (always installed) -has an optional `.spec.pluginRef` field
- **PluginConfig** (optional) -may or may not exist at any point

A single controller reconciles Widgets. When `pluginRef` is set and the PluginConfig CRD exists, it reads the referenced PluginConfig and reports `PluginReady: True`. When the CRD is absent -`PluginReady: False`, reason `PluginCRDNotAvailable`.

```
Startup:
  Widget CRD exists          → watch via builder.For()
  PluginConfig CRD missing   → skip, no watch registered

Runtime -CRD installed:
  CRD watch fires            → reconcile → ensurePluginWatch()
                              → ctrl.Watch(source.Kind(...))

Runtime -CRD removed:
  CRD watch fires            → cache.RemoveInformer()
                              → reset watch flag, requeue Widgets
```

The key bits that make this work:

- **`Build()` not `Complete()`** -retains the `controller.Controller` reference for dynamic `ctrl.Watch()` calls
- **`cache.RemoveInformer()`** (controller-runtime v0.19+) -cleanly stops informers when a CRD disappears
- **Uncached discovery client** -the cached one has a ~10min TTL, which is way too slow for detecting CRD changes
- **CRD name predicate** -scopes the CRD watch to only the optional CRD we care about

## Things that will bite you

These aren't theoretical -each one deadlocked the controller during development.

### `ReaderFailOnMissingInformer` is not optional

Without `ReaderFailOnMissingInformer: true` on the cache, a `r.Get()` call after `RemoveInformer` silently creates a *new* informer for the removed GVK. That informer tries to list/watch against a non-existent API and blocks on `WaitForCacheSync` forever. Your controller is now a very expensive no-op.

### CRD deletion generates spurious events

During CRD deletion, status update events arrive *before* `DeletionTimestamp` is set. If you only check `DeletionTimestamp`, the controller re-registers the watch mid-deletion -creating a race that deadlocks the worker goroutine.

Check both:

```go
crdBeingRemoved := !crd.DeletionTimestamp.IsZero() || !isCRDEstablished(crd)
```

### The `RemoveInformer` / `r.Get` race

If `RemoveInformer` fires between `ensurePluginWatch()` returning `true` and `r.Get(PluginConfig)`, the cache returns `ErrResourceNotCached`. You must catch this, reset the watch flag, and requeue. Miss it and you're back to a deadlocked controller.

## Testing

Three test modes -pick the one that fits your situation:

| Target | What happens |
|---|---|
| `make test` | Embedded apiserver (envtest), in-process manager. Fast, no cluster needed. |
| `make test-int` | Real cluster, in-process manager. Tests real CRD lifecycle and etcd behavior. |
| `make test-e2e` | Real cluster, deployed manager pod. Validates the full stack -image, kustomize, RBAC. |

All three run the same 8 specs covering dynamic watch registration, informer cleanup, the full add/remove/re-add cycle, and pluginRef lifecycle on existing Widgets.

## Try it

### envtest (no cluster needed)

```bash
make test
```

### Against a kind cluster

```bash
make kind-create

# Real cluster, in-process manager
make test-int

# Full e2e -builds image, deploys, tests
make test-e2e

make kind-delete
```

### Poke around manually

```bash
make kind-create
make test-e2e   # deploys the controller

# Create a Widget that references a plugin -CRD doesn't exist yet
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

# Install the CRD at runtime -no restart
kubectl apply -f config/crd/bases/demo.example.com_pluginconfigs.yaml

# Create the PluginConfig it's looking for
kubectl apply -f - <<EOF
apiVersion: demo.example.com/v1alpha1
kind: PluginConfig
metadata:
  name: my-plugin
  namespace: default
spec:
  setting: "hello"
EOF

kubectl get widget test-widget -o jsonpath='{.status.conditions}' | jq .
# → PluginReady: True, reason: PluginApplied

# Pull the rug -remove the CRD entirely
kubectl delete crd pluginconfigs.demo.example.com

kubectl get widget test-widget -o jsonpath='{.status.conditions}' | jq .
# → PluginReady: False, reason: PluginCRDNotAvailable
```

## References

- [controller-runtime PR #2285](https://github.com/kubernetes-sigs/controller-runtime/pull/2285) -`cache.RemoveInformer()`, originated from Gatekeeper's fork
- [controller-runtime Issue #540](https://github.com/kubernetes-sigs/controller-runtime/issues/540) -the original dynamic watch discussion
- [Crossplane Realtime Compositions](https://github.com/crossplane/crossplane/blob/main/internal/controller/apiextensions/definition/reconciler.go) -the most sophisticated dynamic watch implementation out there
- [Gatekeeper Dynamic Cache](https://github.com/open-policy-agent/gatekeeper/tree/master/third_party/sigs.k8s.io/controller-runtime/pkg/dynamiccache) -the fork that inspired upstream support
