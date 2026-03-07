#!/bin/bash
# Sets up a kind cluster with a local Docker registry.
# Follows the official kind local registry guide:
# https://kind.sigs.k8s.io/docs/user/local-registry/
#
# Usage: hack/kind-with-registry.sh [create|delete]
#   create (default) — create registry + cluster
#   delete           — delete cluster (registry is left running)

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-dynamic-crd-watch}"
REGISTRY_NAME="${KIND_REGISTRY_NAME:-kind-registry}"
REGISTRY_PORT="${KIND_REGISTRY_PORT:-5001}"

# --- Helpers ---

ensure_registry() {
    # Check if registry container is already running.
    if [ "$(docker inspect -f '{{.State.Running}}' "${REGISTRY_NAME}" 2>/dev/null)" = "true" ]; then
        echo "Registry ${REGISTRY_NAME} already running on port ${REGISTRY_PORT}"
        return
    fi

    # Start stopped container or create new one.
    if docker inspect "${REGISTRY_NAME}" >/dev/null 2>&1; then
        echo "Starting existing registry container ${REGISTRY_NAME}"
        docker start "${REGISTRY_NAME}"
    else
        echo "Creating registry container ${REGISTRY_NAME} on port ${REGISTRY_PORT}"
        docker run -d \
            --restart=always \
            -p "127.0.0.1:${REGISTRY_PORT}:5000" \
            --network bridge \
            --name "${REGISTRY_NAME}" \
            registry:2
    fi
}

create_cluster() {
    # Skip if cluster already exists.
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        echo "Kind cluster ${CLUSTER_NAME} already exists"
        return
    fi

    echo "Creating kind cluster ${CLUSTER_NAME}"
    cat <<EOF | kind create cluster --name "${CLUSTER_NAME}" --config=-  --wait 2m
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
EOF
}

configure_nodes() {
    echo "Configuring cluster nodes to use registry"
    local registry_dir="/etc/containerd/certs.d/localhost:${REGISTRY_PORT}"

    for node in $(kind get nodes --name "${CLUSTER_NAME}"); do
        docker exec "${node}" mkdir -p "${registry_dir}"
        echo "[host.\"http://${REGISTRY_NAME}:5000\"]" | \
            docker exec -i "${node}" cp /dev/stdin "${registry_dir}/hosts.toml"
    done
}

connect_registry() {
    # Connect registry to kind network (idempotent).
    if ! docker network connect "kind" "${REGISTRY_NAME}" 2>/dev/null; then
        echo "Registry already connected to kind network"
    else
        echo "Connected registry to kind network"
    fi
}

create_configmap() {
    echo "Creating local-registry-hosting ConfigMap"
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${REGISTRY_PORT}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF
}

# --- Main ---

case "${1:-create}" in
    create)
        ensure_registry
        create_cluster
        configure_nodes
        connect_registry
        create_configmap
        echo ""
        echo "Kind cluster '${CLUSTER_NAME}' is ready with local registry at localhost:${REGISTRY_PORT}"
        ;;
    delete)
        echo "Deleting kind cluster ${CLUSTER_NAME}"
        kind delete cluster --name "${CLUSTER_NAME}"
        ;;
    *)
        echo "Usage: $0 [create|delete]" >&2
        exit 1
        ;;
esac
