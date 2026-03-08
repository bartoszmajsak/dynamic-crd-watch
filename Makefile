ENVTEST_K8S_VERSION ?= 1.31.0
KIND_CLUSTER_NAME  ?= dynamic-crd-watch
IMG                ?= localhost:5001/dynamic-crd-watch:test
BUILDER            ?= docker

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.DEFAULT_GOAL := help

include Makefile.tools.mk

# Builder-specific flags: docker requires buildx for BuildKit cache mounts.
ifeq ($(BUILDER),docker)
  BUILD_CMD = $(BUILDER) buildx build
else
  BUILD_CMD = $(BUILDER) build
endif

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CRD and RBAC manifests.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

.PHONY: generate
generate: controller-gen ## Generate DeepCopy methods.
	$(CONTROLLER_GEN) object paths="./..."

.PHONY: lint
lint: golangci-lint ## Run all linters (fmt, vet, golangci-lint).
	go fmt ./...
	go vet ./...
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint with auto-fix.
	$(GOLANGCI_LINT) run --fix

##@ Testing

.PHONY: test
test: manifests generate envtest ## Run tests (embedded apiserver, in-process manager).
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test ./... -coverprofile cover.out

.PHONY: test-int
test-int: ## Run integration tests (real cluster, in-process manager). Requires: make kind-create.
	USE_EXISTING_CLUSTER=true go test -v -count=1 -timeout 10m ./internal/controller/...

.PHONY: test-e2e
test-e2e: image-build image-push deploy ## Run e2e tests (real cluster, deployed manager). Requires: make kind-create.
	USE_EXISTING_CLUSTER=true DEPLOYED_MANAGER=true go test -v -count=1 -timeout 10m ./internal/controller/...

##@ Build

.PHONY: build
build: manifests generate ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate ## Run controller from your host.
	go run ./cmd/main.go

.PHONY: image-build
image-build: manifests generate ## Build container image (BUILDER=docker|podman).
	$(BUILD_CMD) -t $(IMG) .

.PHONY: image-push
image-push: ## Push container image to registry.
	$(BUILDER) push $(IMG)

##@ Cluster

.PHONY: kind-create
kind-create: ## Create a kind cluster with local registry.
	bash hack/kind-with-registry.sh create

.PHONY: kind-delete
kind-delete: ## Delete the kind cluster.
	bash hack/kind-with-registry.sh delete

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster.
	@mkdir -p bin/deploy-overlay && \
	NEW_NAME="$${IMG%:*}" NEW_TAG="$${IMG##*:}" && \
	printf 'resources:\n- ../../config/default\nimages:\n- name: controller\n  newName: %s\n  newTag: %s\n' \
		"$$NEW_NAME" "$$NEW_TAG" > bin/deploy-overlay/kustomization.yaml && \
	$(KUSTOMIZE) build bin/deploy-overlay | kubectl apply --server-side -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster.
	$(KUSTOMIZE) build config/default | kubectl delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: validate-manifests
validate-manifests: manifests kustomize ## Validate all kustomize manifests.
	PATH=$(LOCALBIN):$$PATH bash hack/tasks/lint/manifests

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster.
	$(KUSTOMIZE) build config/crd | kubectl apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster.
	$(KUSTOMIZE) build config/crd | kubectl delete --ignore-not-found=$(ignore-not-found) -f -
