// This file lives in package dynamicwatch (not dynamicwatch_test) so it can
// reach unexported fields and methods of Watcher. Go only compiles it during
// `go test`, so none of this leaks into production binaries.
//
// The watcher_test.go file is a black-box test (package dynamicwatch_test)
// that imports these helpers to exercise internal behavior - like injecting
// a fake CRD availability check or simulating a CRD lifecycle event -
// without coupling tests to the struct layout.
//
// This is the standard Go "export_test.go" pattern used by the stdlib
// and controller-runtime itself.
package dynamicwatch

import (
	"context"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewTestWatcher creates a Watcher for unit testing without requiring a
// ctrl.Manager. In production, Build() derives the GVK from the scheme,
// creates a discovery client, etc. - none of which is needed (or desirable)
// in a unit test.
//
// Only the essential dependencies (cache, controller) are parameters.
// GVK, object mapper, requeue function, and newT are defaulted to safe
// no-ops. Use SetCRDAvailable and SetRequeueAll to override behavior
// for specific test scenarios.
//
// The ctrl parameter may be nil to test the "Bind not called" code path.
func NewTestWatcher[T client.Object](
	crdName string,
	c cache.Cache,
	ctrl WatchRegistrar,
) *Watcher[T] {
	return &Watcher[T]{
		crdName:      crdName,
		cache:        c,
		ctrl:         ctrl,
		crdAvailable: func(_ context.Context) bool { return false },
		objectMapper: func(_ context.Context, _ T) []reconcile.Request { return nil },
		requeueAll:   func(_ context.Context) []reconcile.Request { return nil },
		newT:         newInstance[T],
	}
}

// SetCRDAvailable replaces the discovery-based CRD check with fn.
// This lets tests control what Ensure() sees without hitting an API server.
func SetCRDAvailable[T client.Object](w *Watcher[T], fn func(context.Context) bool) {
	w.crdAvailable = fn
}

// SetRequeueAll replaces the lifecycle requeue function. Use this in tests
// that need to verify whether onCRDChange triggers a requeue and with
// what requests.
func SetRequeueAll[T client.Object](w *Watcher[T], fn RequeueParentsFn) {
	w.requeueAll = fn
}

// SetActive forces the watch into the given state. Useful for setting up
// preconditions like "watch is already registered" before testing Get,
// Ensure, or onCRDChange behavior.
func SetActive[T client.Object](w *Watcher[T], active bool) {
	w.active = active
}

// SimulateCRDChange calls the unexported onCRDChange handler directly,
// bypassing the controller-runtime event pipeline. This lets tests verify
// CRD removal/addition logic (informer teardown, requeue decisions) in
// isolation, without needing a running informer for the CRD type.
func SimulateCRDChange[T client.Object](w *Watcher[T], ctx context.Context, crd *apiextensionsv1.CustomResourceDefinition) []reconcile.Request {
	return w.onCRDChange(ctx, crd)
}
