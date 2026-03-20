// Package dynamicwatch manages watches for optional CRDs that may be
// installed or removed at runtime. Each [Watcher] tracks one CRD's lifecycle -
// registering an informer when the CRD appears and tearing it down when it
// disappears - without requiring a controller restart.
//
// Each Watcher creates a private cache for its object type T, separate from
// the manager's shared cache. This is critical for multi-controller managers:
// if two controllers watch the same optional GVK and one removes the informer,
// a shared cache would break both controllers. The private cache ensures that
// each Watcher's [cache.Cache.RemoveInformer] call only affects its own
// informer. The private cache is configured with
// [cache.Options.ReaderFailOnMissingInformer] internally - callers don't need
// to set any special options on their manager cache.
//
// A Watcher implements [source.SyncingSource], so it plugs directly into the
// controller builder:
//
//	w, err := dynamicwatch.For[*v1alpha1.PluginConfig](mgr, "pluginconfigs.demo.example.com").
//	    WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(r.pluginConfigToWidgets)).
//	    EnqueueOnCRDChange(r.allWidgetsWithPluginRef).
//	    Build()
//
//	ctrl.NewControllerManagedBy(mgr).
//	    For(&v1alpha1.Widget{}).
//	    WatchesRawSource(w).
//	    Complete(r)
package dynamicwatch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ErrCacheInvalidated is returned by [Watcher.Get] when the informer was
// removed between [Watcher.Ensure] and the read. The watcher resets its
// internal state automatically - the caller should requeue.
var ErrCacheInvalidated = errors.New("informer removed during operation")

// RequeueParentsFn returns reconcile requests for all parent objects affected
// by a CRD lifecycle change (appearance or removal).
type RequeueParentsFn func(ctx context.Context) []reconcile.Request

// CRDCache is a shared cache for CustomResourceDefinition objects.
// Create one per manager via [NewCRDCache] and pass it to all Watchers
// via [WatcherBuilder.WithCRDCache]. This avoids one LIST/WATCH connection
// per watcher - all watchers share a single CRD informer and filter
// events client-side via name predicates.
type CRDCache struct {
	cache cache.Cache
}

// NewCRDCache creates a shared CRD cache and registers it with the manager.
// The cache watches all CRDs (no field selector); each watcher adds its
// own name predicate.
func NewCRDCache(mgr ctrl.Manager) (*CRDCache, error) {
	if mgr == nil {
		return nil, errors.New("dynamicwatch: manager is required")
	}

	c, err := cache.New(mgr.GetConfig(), cache.Options{
		HTTPClient: mgr.GetHTTPClient(),
		Scheme:     mgr.GetScheme(),
		Mapper:     mgr.GetRESTMapper(),
		ByObject: map[client.Object]cache.ByObject{
			&apiextensionsv1.CustomResourceDefinition{}: {
				Transform: transformCRDForCache,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating shared CRD cache: %w", err)
	}

	if err := mgr.Add(c); err != nil {
		return nil, fmt.Errorf("registering shared CRD cache: %w", err)
	}

	return &CRDCache{cache: c}, nil
}

// Compile-time check: Watcher must satisfy source.SyncingSource.
var _ source.SyncingSource = (*Watcher[client.Object])(nil)

// Watcher manages the lifecycle of a watch for an optional CRD of type T.
// It implements [source.SyncingSource] so it can be passed directly to
// [builder.Builder.WatchesRawSource].
type Watcher[T client.Object] struct {
	crdName    string
	gvk        schema.GroupVersionKind
	objCache   cache.Cache
	crdCache   cache.Cache
	objHandler handler.TypedEventHandler[T, reconcile.Request]
	predicates []predicate.TypedPredicate[T]
	requeueAll RequeueParentsFn
	newT       func() T

	// startSource is set by Start. It starts a sub-source using the
	// controller's lifecycle context and queue, both captured at Start time.
	startSource func(src source.SyncingSource) error
	// startSyncWaiter is set by Start. It spawns a goroutine that blocks
	// until the source's informer has synced, then promotes to active and
	// enqueues all affected parents. Nil in unit tests.
	startSyncWaiter func(gen uint64, src source.SyncingSource)
	crdSrc          source.SyncingSource

	mu               sync.Mutex
	watching         bool // source registered via Watch (may not be synced yet)
	active           bool // informer synced - safe to read
	crdExists        bool
	generation       uint64 // incremented on teardown, prevents stale sync waiters from promoting
	cancelSyncWaiter context.CancelFunc
}

// Start is called by the controller framework when it starts. It stores
// the queue and context for later use in [Watcher.Ensure], then creates
// and starts the CRD sub-source that drives [Watcher.onCRDChange].
//
// This method satisfies [source.TypedSource] and must not be called directly.
func (w *Watcher[T]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	if w.startSource != nil {
		return errors.New("dynamicwatch: Start called twice")
	}

	w.startSource = func(src source.SyncingSource) error {
		return src.Start(ctx, queue)
	}

	w.startSyncWaiter = func(gen uint64, src source.SyncingSource) {
		w.waitForSyncAndRequeue(ctx, queue, gen, src)
	}

	w.crdSrc = source.Kind(w.crdCache, &apiextensionsv1.CustomResourceDefinition{},
		handler.TypedEnqueueRequestsFromMapFunc(w.onCRDChange),
		crdNamePredicate(w.crdName),
	)

	return w.crdSrc.Start(ctx, queue)
}

// WaitForSync blocks until the CRD informer has synced, ensuring that the
// [Watcher.crdExists] flag is accurate from the very first reconcile.
//
// This method satisfies [source.SyncingSource].
func (w *Watcher[T]) WaitForSync(ctx context.Context) error {
	if w.crdSrc == nil {
		return errors.New("dynamicwatch: WaitForSync called before Start")
	}

	return w.crdSrc.WaitForSync(ctx)
}

// Ensure checks CRD availability and registers the watch if needed.
// CRD availability is tracked via an event-driven flag set by
// [Watcher.onCRDChange] - no discovery API calls are made.
//
// Returns true when the watch is active and the informer cache has synced
// (safe to read). Returns false when the CRD is not installed or the
// informer is still syncing.
//
// When a new watch is registered and the informer has not synced yet,
// Ensure spawns a background goroutine that waits for sync and then
// enqueues all affected parent objects via [RequeueParentsFn]. This
// eliminates the need for callers to implement their own requeue-on-sync
// logic - just return early on false and the watcher will trigger a
// re-reconcile once the cache is ready.
func (w *Watcher[T]) Ensure(ctx context.Context) bool {
	if w.startSource == nil {
		logf.FromContext(ctx).Error(nil, "Start() not called, cannot register watch", "crd", w.crdName)

		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active {
		return true
	}

	if !w.crdExists {
		return false
	}

	log := logf.FromContext(ctx)

	if !w.watching {
		src := source.Kind(w.objCache, w.newT(), w.objHandler, w.predicates...)

		if err := w.startSource(src); err != nil {
			log.Error(err, "Failed to start dynamic watch", "crd", w.crdName)

			return false
		}

		w.watching = true

		if w.startSyncWaiter != nil {
			// Cancel any previous sync waiter that might still be blocking
			// (e.g. after a timeout-and-retry cycle).
			if w.cancelSyncWaiter != nil {
				w.cancelSyncWaiter()
			}

			go w.startSyncWaiter(w.generation, src)
		}

		log.Info("Started dynamic watch, waiting for informer sync", "crd", w.crdName)
	}

	return false
}

// Get reads an object of type T from the cache. If the informer was removed
// between [Watcher.Ensure] and this call (the RemoveInformer/Get race), the
// watcher resets its internal state and returns [ErrCacheInvalidated]. The
// caller should requeue.
//
// All other errors (including NotFound) are returned as-is.
func (w *Watcher[T]) Get(ctx context.Context, key client.ObjectKey, obj T) error {
	if err := w.objCache.Get(ctx, key, obj); err != nil {
		var notCached *cache.ErrResourceNotCached
		if errors.As(err, &notCached) {
			w.mu.Lock()
			w.watching = false
			w.active = false

			// Reset crdExists because ErrResourceNotCached only occurs after
			// onCRDChange called RemoveInformer due to CRD removal. A subsequent
			// CRD re-installation will trigger a new onCRDChange event that sets
			// crdExists=true again.
			w.crdExists = false
			w.generation++

			if w.cancelSyncWaiter != nil {
				w.cancelSyncWaiter()
				w.cancelSyncWaiter = nil
			}
			w.mu.Unlock()

			return ErrCacheInvalidated
		}

		return err
	}

	return nil
}

// Available reports whether the watch is currently active. This is a
// point-in-time snapshot - the CRD may be removed immediately after.
func (w *Watcher[T]) Available() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.active
}

// onCRDChange handles CRD lifecycle events for the target CRD.
//
// When the CRD is being removed (DeletionTimestamp set or Established
// condition is False), the informer is torn down and affected objects
// are requeued so they re-evaluate their state.
//
// When the CRD is being removed but the watch was never active (e.g.
// the CRD was installed and removed before any reconcile called Ensure),
// the handler returns nil - there are no informers to tear down and no
// cached objects to invalidate, so requeueing would only cause
// unnecessary reconciles that hit Ensure() → Unavailable.
//
// The Established condition check catches CRD deletion mid-flight:
// during deletion, Kubernetes updates the CRD status (setting
// Established=False) before setting DeletionTimestamp. Without this
// check, status update events would trigger spurious watch
// re-registration that deadlocks on WaitForCacheSync.
func (w *Watcher[T]) onCRDChange(ctx context.Context, crd *apiextensionsv1.CustomResourceDefinition) []reconcile.Request {
	log := logf.FromContext(ctx)

	crdRemoved := !crd.DeletionTimestamp.IsZero() || !isCRDEstablished(crd)
	if crdRemoved {
		w.mu.Lock()
		wasWatching := w.watching
		wasReady := w.active
		w.watching = false
		w.active = false
		w.crdExists = false

		w.generation++

		// Cancel any in-flight sync waiter so it bails immediately instead
		// of blocking until the timeout expires. The generation increment
		// above prevents stale promotion; this just makes it faster.
		if w.cancelSyncWaiter != nil {
			w.cancelSyncWaiter()
			w.cancelSyncWaiter = nil
		}
		w.mu.Unlock()

		if wasWatching {
			// RemoveInformer is called outside the mutex to avoid deadlocks with
			// potentially blocking cache operations. This is safe because
			// controller-runtime's internal informer map has its own lock, and
			// Get's ErrResourceNotCached handling serves as a safety net for
			// any race between RemoveInformer and concurrent Ensure/Get calls.
			if err := w.objCache.RemoveInformer(ctx, w.newT()); err != nil {
				log.Error(err, "Failed to remove informer", "crd", w.crdName)
			}

			log.Info("Removed watch after CRD deletion", "crd", w.crdName)
		}

		// Only requeue if the watch was active - if it was never registered,
		// there are no cached objects to invalidate and no state to re-evaluate.
		if !wasReady {
			return nil
		}
	} else {
		w.mu.Lock()
		alreadyKnown := w.crdExists
		w.crdExists = true
		w.mu.Unlock()

		if alreadyKnown {
			return nil
		}

		log.Info("CRD detected, will requeue affected objects", "crd", w.crdName)
	}

	return w.requeueAll(ctx)
}

// informerSyncTimeout is a safety net for rare cases where the informer
// gets stuck syncing (e.g. misbehaving API server). In normal operation
// sync completes in milliseconds - this just prevents the goroutine from
// blocking for the entire manager lifetime with no signal.
const informerSyncTimeout = 30 * time.Second

// waitForSyncAndRequeue blocks until the object cache has synced, then
// promotes the watcher to active and enqueues all affected parent objects.
// It is spawned as a goroutine by [Watcher.Ensure] when a new watch is
// registered.
//
// The generation parameter guards against stale promotions: if the CRD is
// removed while this method is blocking, [Watcher.onCRDChange] increments
// the generation and the stale goroutine bails without promoting.
func (w *Watcher[T]) waitForSyncAndRequeue(
	ctx context.Context,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
	gen uint64,
	src source.SyncingSource,
) {
	log := logf.FromContext(ctx)

	syncCtx, cancel := context.WithTimeout(ctx, informerSyncTimeout)

	// Store the cancel func so Ensure() / onCRDChange can abort this goroutine
	// before spawning a replacement or tearing down the watch.
	w.mu.Lock()
	w.cancelSyncWaiter = cancel
	w.mu.Unlock()

	// Wait on the specific source - not the cache. source.Kind.WaitForSync
	// blocks until GetInformer completes AND the informer has synced AND the
	// event handler has received all initial events. In contrast,
	// cache.WaitForCacheSync returns true immediately when no informers are
	// registered, which races with the async GetInformer call inside
	// source.Kind.Start.
	syncErr := src.WaitForSync(syncCtx)
	cancel()

	if syncErr != nil {
		// Timeout, context cancelled, or source startup failure.
		w.mu.Lock()
		stale := w.generation != gen
		if !stale {
			w.watching = false
			log.Info("Informer sync failed, will retry on next Ensure()", "crd", w.crdName, "error", syncErr)
		}
		w.mu.Unlock()

		if stale {
			// Generation advanced (CRD removed while we were syncing).
			// onCRDChange already handled cleanup - don't touch the informer.
			return
		}

		// Remove the stale informer outside the mutex (same pattern as
		// onCRDChange). Without this, the next Ensure() would register a
		// second event handler on the existing informer, stacking handlers
		// on repeated timeouts.
		if err := w.objCache.RemoveInformer(ctx, w.newT()); err != nil {
			log.Error(err, "Failed to remove informer after sync failure", "crd", w.crdName)
		}

		// Enqueue parents so they retry. Without this, a transient API
		// server issue + GenerationChangedPredicate could leave affected
		// objects stuck indefinitely in CRDNotAvailable.
		for _, req := range w.requeueAll(ctx) {
			queue.Add(req)
		}

		return
	}

	w.mu.Lock()
	if w.generation != gen {
		w.mu.Unlock()

		return
	}

	w.active = true
	w.mu.Unlock()

	log.Info("Dynamic watch ready", "crd", w.crdName)

	for _, req := range w.requeueAll(ctx) {
		queue.Add(req)
	}
}

// crdNamePredicate returns a predicate that matches CRDs by metadata.name.
// Used to filter shared CRD cache events to the specific CRD this watcher
// is tracking.
func crdNamePredicate(name string) predicate.TypedPredicate[*apiextensionsv1.CustomResourceDefinition] {
	return predicate.NewTypedPredicateFuncs(func(crd *apiextensionsv1.CustomResourceDefinition) bool {
		return crd.Name == name
	})
}

// transformCRDForCache is a cache transform that strips bulky fields from CRD objects
// before they are stored in the informer's in-memory store. It removes spec
// (large OpenAPI schemas), managed fields, and annotations (which often carry
// kubectl.kubernetes.io/last-applied-configuration duplicating the entire spec
// as JSON). We only need metadata (name, deletionTimestamp) and status
// (Established condition).
func transformCRDForCache(obj any) (any, error) {
	if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
		crd.Spec = apiextensionsv1.CustomResourceDefinitionSpec{}
		crd.ManagedFields = nil
		crd.Annotations = nil
	}

	return obj, nil
}

// newInstance creates a zero-value instance of T using reflection.
// T must be a pointer type (e.g. *PluginConfig) as required by client.Object.
func newInstance[T client.Object]() T {
	var zero T
	typ := reflect.TypeOf(zero).Elem()

	//nolint:errcheck,forcetypeassert // Type assertion is guaranteed safe: T is always a pointer to a
	// concrete type implementing client.Object, and reflect.New produces exactly that type.
	return reflect.New(typ).Interface().(T)
}

func isCRDEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, c := range crd.Status.Conditions {
		if c.Type == apiextensionsv1.Established {
			return c.Status == apiextensionsv1.ConditionTrue
		}
	}

	return false
}
