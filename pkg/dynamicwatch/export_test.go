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
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// WatcherActive exports the gauge metric for testing.
var WatcherActive = watcherActive

// WatcherTransitions exports the counter metric for testing.
var WatcherTransitions = watcherTransitions

// NewTestWatcher creates a Watcher for unit testing without requiring a
// ctrl.Manager. Sets started=true and stores the given ctx so Ensure()
// works without calling Start().
func NewTestWatcher[T client.Object](
	crdName string,
	c cache.Cache,
	ctx context.Context,
) *Watcher[T] {
	w := &Watcher[T]{
		crdName:     crdName,
		objCache:    c,
		ctx:         ctx,
		syncTimeout: defaultSyncTimeout,
		objHandler: handler.TypedEnqueueRequestsFromMapFunc(
			func(_ context.Context, _ T) []reconcile.Request { return nil },
		),
		requeueAll: func(_ context.Context) []reconcile.Request { return nil },
		newT:       newInstance[T],
	}
	w.started.Store(true)

	return w
}

// NewUnstartedTestWatcher creates a Watcher without setting started=true.
// Use this to test the Ensure-before-Start panic path.
func NewUnstartedTestWatcher[T client.Object](
	crdName string,
	c cache.Cache,
) *Watcher[T] {
	return &Watcher[T]{
		crdName:     crdName,
		objCache:    c,
		syncTimeout: defaultSyncTimeout,
		objHandler: handler.TypedEnqueueRequestsFromMapFunc(
			func(_ context.Context, _ T) []reconcile.Request { return nil },
		),
		requeueAll: func(_ context.Context) []reconcile.Request { return nil },
		newT:       newInstance[T],
	}
}

// SetStarted marks the watcher as started with the given context.
// Use this when a test needs to transition from unstarted to started
// without going through the real Start() method.
func SetStarted[T client.Object](w *Watcher[T], ctx context.Context) {
	w.ctx = ctx
	w.started.Store(true)
}

// SetCRDExists sets the event-driven CRD existence flag.
// This lets tests control what Ensure() sees without a running CRD cache.
func SetCRDExists[T client.Object](w *Watcher[T], exists bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.crdExists = exists
}

// SetRequeueAll replaces the lifecycle requeue function. Use this in tests
// that need to verify whether onCRDChange triggers a requeue and with
// what requests.
func SetRequeueAll[T client.Object](w *Watcher[T], fn RequeueParentsFn) {
	w.requeueAll = fn
}

// SetActive forces the watch into the given state. Useful for setting up
// preconditions like "watch is already registered" before testing TryGet,
// Ensure, or onCRDChange behavior. Setting active=true also sets the
// watching flag, since active implies watching.
func SetActive[T client.Object](w *Watcher[T], active bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.active = active
	if active {
		w.watching = true
	}
}

// SetWatching forces the watching flag. Use this to simulate the state where
// a watch source has been registered but the informer hasn't synced yet.
func SetWatching[T client.Object](w *Watcher[T], watching bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.watching = watching
}

// SetGeneration sets the generation counter. Use this to test that stale
// sync waiters don't promote the watcher after a teardown.
func SetGeneration[T client.Object](w *Watcher[T], gen uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.generation = gen
}

// Generation returns the current generation counter value.
func Generation[T client.Object](w *Watcher[T]) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.generation
}

// SetQueue sets the work queue on the watcher. Tests that need to inspect
// queue contents or whose code paths call w.queue.Add should set this.
func SetQueue[T client.Object](w *Watcher[T], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	w.queue = q
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

// CallWaitForSyncAndRequeue exposes waitForSyncAndRequeue for direct testing.
func CallWaitForSyncAndRequeue[T client.Object](
	w *Watcher[T], syncCtx context.Context, syncCancel context.CancelFunc,
	gen uint64, src source.SyncingSource,
) {
	w.waitForSyncAndRequeue(syncCtx, syncCancel, gen, src)
}

// SyncTimeout returns the configured sync timeout for the watcher.
func SyncTimeout[T client.Object](w *Watcher[T]) time.Duration {
	return w.syncTimeout
}

// SetRecorder sets the event recorder on the watcher for testing.
func SetRecorder[T client.Object](w *Watcher[T], recorder record.EventRecorder) {
	w.recorder = recorder
}
