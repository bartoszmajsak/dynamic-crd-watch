# Development

## Build options

Two build systems are available - use whichever you prefer:

| | mise | make |
|---|---|---|
| Setup | `mise install` | tools downloaded on first use |
| Run task | `mise run <task>` | `make <target>` |
| Tool versions | `mise.toml` | `Makefile.tools.mk` |

mise handles tool installation automatically (`go`, `controller-gen`, `kustomize`, `golangci-lint`, `setup-envtest`). With make, tools are downloaded to `bin/` on first use.

### mise tasks

```
mise tasks                 # list all available tasks

mise run build             # build manager binary
mise run lint              # run all linters
mise run test              # envtest
mise run test:int          # integration (real cluster, in-process manager)
mise run test:e2e          # e2e (real cluster, deployed manager)
mise run image:build       # build container image
mise run deploy            # deploy to cluster
mise run kind:create       # create kind cluster with local registry
```

Container builder is configurable via `BUILDER` env var (defaults to `docker`):

```bash
BUILDER=podman mise run image:build
```

### make targets

```
make build                 # build manager binary
make lint                  # run all linters
make test                  # envtest
make test-int              # integration
make test-e2e              # e2e
make image-build           # build container image (BUILDER=docker|podman)
make deploy                # deploy to cluster
make kind-create           # create kind cluster with local registry
```

## Testing

Three test modes, controlled by environment variables:

| Mode | What it tests | Cluster needed? |
|---|---|---|
| **envtest** (default) | Embedded apiserver, in-process manager. Fast. | No |
| **integration** (`USE_EXISTING_CLUSTER`) | Real apiserver + etcd, in-process manager. Tests real CRD lifecycle. | Yes |
| **e2e** (`USE_EXISTING_CLUSTER` + `DEPLOYED_MANAGER`) | Real cluster, deployed manager pod. Validates image, kustomize, RBAC. | Yes |

All three run the same 8 specs covering dynamic watch registration, informer cleanup, the full add/remove/re-add cycle, and pluginRef lifecycle on existing Widgets.

### Running against a kind cluster

```bash
# Create cluster with local registry
mise run kind:create       # or: make kind-create

# Integration tests (in-process manager against real cluster)
mise run test:int          # or: make test-int

# Full e2e (build image, deploy, test against deployed manager)
mise run test:e2e          # or: make test-e2e

# Clean up
mise run kind:delete       # or: make kind-delete
```

### Manual exploration

```bash
mise run kind:create
mise run deploy

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
  namespace: default
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
