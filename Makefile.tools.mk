## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBEBUILDER    ?= $(LOCALBIN)/kubebuilder
KUSTOMIZE      ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST        ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT  ?= $(LOCALBIN)/golangci-lint

## Tool Versions
KUBEBUILDER_VERSION      ?= v4.12.0
KUSTOMIZE_VERSION        ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1
ENVTEST_VERSION          ?= release-0.22
GOLANGCI_LINT_VERSION    ?= v2.11.1

KUBEBUILDER_OS   ?= $(shell go env GOOS)
KUBEBUILDER_ARCH ?= $(shell go env GOARCH)

.PHONY: kubebuilder
kubebuilder: $(KUBEBUILDER)
$(KUBEBUILDER): $(LOCALBIN)
	@[ -f "$(KUBEBUILDER)-$(KUBEBUILDER_VERSION)" ] || { \
		set -e; \
		echo "Installing kubebuilder $(KUBEBUILDER_VERSION)"; \
		curl -fsSL -o $(KUBEBUILDER) https://github.com/kubernetes-sigs/kubebuilder/releases/download/$(KUBEBUILDER_VERSION)/kubebuilder_$(KUBEBUILDER_OS)_$(KUBEBUILDER_ARCH); \
		chmod +x $(KUBEBUILDER); \
		mv $(KUBEBUILDER) $(KUBEBUILDER)-$(KUBEBUILDER_VERSION); \
	}
	@ln -sf $(KUBEBUILDER)-$(KUBEBUILDER_VERSION) $(KUBEBUILDER)

.PHONY: kustomize
kustomize: $(KUSTOMIZE)
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: envtest
envtest: $(ENVTEST)
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT)
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool installs a Go binary at the given path, versioned to avoid re-downloads.
# Usage: $(call go-install-tool,<binary-path>,<package>,<version>)
define go-install-tool
@rm -f $(1)
@[ -f "$(1)-$(3)" ] || { \
	set -e; \
	echo "Installing $(2)@$(3)"; \
	GOBIN=$(LOCALBIN) go install $(2)@$(3); \
	mv $(1) $(1)-$(3); \
}
@ln -sf $(1)-$(3) $(1)
endef
