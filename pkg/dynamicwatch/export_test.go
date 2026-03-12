// This file lives in package dynamicwatch (not dynamicwatch_test) so it can
// reach unexported fields and methods of Watcher. Go only compiles it during
// `go test`, so none of this leaks into production binaries.
//
// The watcher_test.go file is a black-box test (package dynamicwatch_test)
// that imports these helpers to exercise internal behavior - like setting
// the crdExists flag or simulating a CRD lifecycle event -
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// NewTestWatcher creates a Watcher for unit testing without requiring a
// ctrl.Manager. In production, Build() derives the GVK from the scheme,
// creates a CRD objCache, etc. - none of which is needed (or desirable)
// in a unit test.
//
// Only the essential dependency (cache) is a parameter. GVK, event handler,
// requeue function, and newT are defaulted to safe no-ops. Use
// SetCRDExists, SetRequeueAll, and SetStartSource to override behavior
// for specific test scenarios.
func NewTestWatcher[T client.Object](
	crdName string,
	c cache.Cache,
) *Watcher[T] {
	return &Watcher[T]{
		crdName:  crdName,
		objCache: c,
		objHandler: handler.TypedEnqueueRequestsFromMapFunc(
			func(_ context.Context, _ T) []reconcile.Request { return nil },
		),
		requeueAll: func(_ context.Context) []reconcile.Request { return nil },
		newT:       newInstance[T],
	}
}

// SetStartSource replaces the startSource closure directly. Use this to
// inject failures or count calls in tests that need fine-grained control
// over the source startup behavior.
func SetStartSource[T client.Object](w *Watcher[T], fn func(src source.SyncingSource) error) {
	w.startSource = fn
}

// SetCRDExists sets the event-driven CRD existence flag.
// This lets tests control what Ensure() sees without a running CRD cache.
func SetCRDExists[T client.Object](w *Watcher[T], exists bool) {
	w.crdExists = exists
}

// SetRequeueAll replaces the lifecycle requeue function. Use this in tests
// that need to verify whether onCRDChange triggers a requeue and with
// what requests.
func SetRequeueAll[T client.Object](w *Watcher[T], fn RequeueParentsFn) {
	w.requeueAll = fn
}

// SetActive forces the watch into the given state. Useful for setting up
// preconditions like "watch is already registered" before testing Get,
// Ensure, or onCRDChange behavior. Setting active=true also sets the
// watching flag, since active implies watching.
func SetActive[T client.Object](w *Watcher[T], active bool) {
	w.active = active
	if active {
		w.watching = true
	}
}

// SetWatching forces the watching flag. Use this to simulate the state where
// a watch source has been registered but the informer hasn't synced yet.
func SetWatching[T client.Object](w *Watcher[T], watching bool) {
	w.watching = watching
}

// SetGeneration sets the generation counter. Use this to test that stale
// sync waiters don't promote the watcher after a teardown.
func SetGeneration[T client.Object](w *Watcher[T], gen uint64) {
	w.generation = gen
}

// Generation returns the current generation counter value.
func Generation[T client.Object](w *Watcher[T]) uint64 {
	return w.generation
}

// SetStartSyncWaiter replaces the startSyncWaiter closure. Use this in
// tests that need to exercise the background sync waiter goroutine
// (waitForSyncAndRequeue) through Ensure().
func SetStartSyncWaiter[T client.Object](w *Watcher[T], fn func(gen uint64, src source.SyncingSource)) {
	w.startSyncWaiter = fn
}

// Watching returns the current watching flag under the mutex.
func Watching[T client.Object](w *Watcher[T]) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.watching
}

// SimulateCRDChange calls the unexported onCRDChange handler directly,
// bypassing the controller-runtime event pipeline. This lets tests verify
// CRD removal/addition logic (informer teardown, requeue decisions) in
// isolation, without needing a running informer for the CRD type.
func SimulateCRDChange[T client.Object](w *Watcher[T], ctx context.Context, crd *apiextensionsv1.CustomResourceDefinition) []reconcile.Request {
	return w.onCRDChange(ctx, crd)
}
