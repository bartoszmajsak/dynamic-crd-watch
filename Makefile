ENVTEST_K8S_VERSION ?= 1.31.0
KIND_CLUSTER_NAME  ?= dynamic-crd-watch

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.DEFAULT_GOAL := help

include Makefile.tools.mk

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CRD manifests.
	$(CONTROLLER_GEN) crd paths="./..." output:crd:artifacts:config=config/crd/bases

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

.PHONY: test
test: manifests generate envtest ## Run tests against envtest (embedded apiserver).
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test ./... -coverprofile cover.out

.PHONY: test-kind
test-kind: manifests generate ## Run tests against a kind cluster (USE_EXISTING_CLUSTER=true).
	USE_EXISTING_CLUSTER=true go test ./... -coverprofile cover.out

##@ Build

.PHONY: build
build: manifests generate ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate ## Run controller from your host.
	go run ./cmd/main.go

##@ Cluster

.PHONY: kind-create
kind-create: ## Create a kind cluster.
	kind create cluster --name $(KIND_CLUSTER_NAME)

.PHONY: kind-delete
kind-delete: ## Delete the kind cluster.
	kind delete cluster --name $(KIND_CLUSTER_NAME)

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: validate-manifests
validate-manifests: manifests kustomize ## Validate all kustomize manifests.
	PATH=$(LOCALBIN):$$PATH bash hack/validate-manifests.sh

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster.
	$(KUSTOMIZE) build config/crd | kubectl apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster.
	$(KUSTOMIZE) build config/crd | kubectl delete --ignore-not-found=$(ignore-not-found) -f -
