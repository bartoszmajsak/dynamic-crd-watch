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
// controller builder. Use [Watcher.TryGet] and [Watcher.TryList] in your
// reconciler to read objects from the dynamic cache - they handle CRD
// availability checks, informer lifecycle, and cache invalidation races
// internally.
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
	"k8s.io/apimachinery/pkg/api/meta"
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

// errCacheInvalidated is returned by [Watcher.get] when the informer was
// removed between [Watcher.Ensure] and the read. The watcher resets its
// internal state automatically - the caller should requeue.
var errCacheInvalidated = errors.New("informer removed during operation")

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
	crdName     string
	objCache    cache.Cache
	crdCache    cache.Cache
	objHandler  handler.TypedEventHandler[T, reconcile.Request]
	predicates  []predicate.TypedPredicate[T]
	requeueAll  RequeueParentsFn
	newT        func() T
	syncTimeout time.Duration

	// ctx is the controller lifecycle context, set by Start.
	ctx context.Context //nolint:containedctx // Intentional: goroutines spawned by Ensure need the lifecycle context.
	// queue is the controller's work queue, set by Start.
	queue   workqueue.TypedRateLimitingInterface[reconcile.Request]
	started bool
	crdSrc  source.SyncingSource

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
	if w.started {
		return errors.New("dynamicwatch: Start called twice")
	}

	w.ctx = ctx
	w.queue = queue

	w.crdSrc = source.Kind(w.crdCache, &apiextensionsv1.CustomResourceDefinition{},
		handler.TypedEnqueueRequestsFromMapFunc(w.onCRDChange),
		crdNamePredicate(w.crdName),
	)

	if err := w.crdSrc.Start(ctx, queue); err != nil {
		return err
	}

	w.started = true

	return nil
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
//
// Panics if called before Start.
func (w *Watcher[T]) Ensure(ctx context.Context) bool {
	if !w.started {
		panic("dynamicwatch: Ensure called before Start")
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

		if err := src.Start(w.ctx, w.queue); err != nil { //nolint:contextcheck // w.ctx is the lifecycle context from Start, not the per-reconcile ctx.
			log.Error(err, "Failed to start dynamic watch", "crd", w.crdName)

			return false
		}

		w.watching = true

		if w.cancelSyncWaiter != nil {
			w.cancelSyncWaiter()
		}

		syncCtx, syncCancel := context.WithTimeout(w.ctx, w.syncTimeout)
		w.cancelSyncWaiter = syncCancel

		go w.waitForSyncAndRequeue(syncCtx, syncCancel, w.generation, src) //nolint:contextcheck // Timeout derived from lifecycle context, not per-reconcile ctx.

		log.Info("Started dynamic watch, waiting for informer sync", "crd", w.crdName)
	}

	return false
}

// TryGet combines [Watcher.Ensure] and a cache read in one call. It returns
// (true, nil) when the object was found, (true, err) on cache errors other
// than informer removal, and (false, nil) when the CRD is unavailable or
// the informer was removed mid-read. On false, the caller should skip
// the optional-CRD logic and return early - the watcher will requeue
// automatically once the cache is ready again.
func (w *Watcher[T]) TryGet(ctx context.Context, key client.ObjectKey, obj T) (bool, error) {
	if !w.Ensure(ctx) {
		return false, nil
	}

	if err := w.get(ctx, key, obj); err != nil {
		if errors.Is(err, errCacheInvalidated) {
			return false, nil
		}

		return true, err
	}

	return true, nil
}

// TryList combines [Watcher.Ensure] and a cache list in one call. It returns
// (true, nil) when the list succeeded, (true, err) on cache errors other
// than informer removal, and (false, nil) when the CRD is unavailable or
// the informer was removed mid-read. On false, the list is cleared and
// the caller should skip the optional-CRD logic.
func (w *Watcher[T]) TryList(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (bool, error) {
	if !w.Ensure(ctx) {
		clearList(list)

		return false, nil
	}

	if err := w.list(ctx, list, opts...); err != nil {
		if errors.Is(err, errCacheInvalidated) {
			clearList(list)

			return false, nil
		}

		return true, err
	}

	return true, nil
}

// get reads an object of type T from the cache. If the informer was removed
// between [Watcher.Ensure] and this call (the RemoveInformer/get race), the
// watcher resets its internal state and returns [errCacheInvalidated]. The
// caller should requeue.
//
// All other errors (including NotFound) are returned as-is.
func (w *Watcher[T]) get(ctx context.Context, key client.ObjectKey, obj T) error {
	if err := w.objCache.Get(ctx, key, obj); err != nil {
		var notCached *cache.ErrResourceNotCached
		if errors.As(err, &notCached) {
			w.handleCacheInvalidated()

			return errCacheInvalidated
		}

		return err
	}

	return nil
}

func (w *Watcher[T]) list(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := w.objCache.List(ctx, list, opts...); err != nil {
		var notCached *cache.ErrResourceNotCached
		if errors.As(err, &notCached) {
			w.handleCacheInvalidated()

			return errCacheInvalidated
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

// handleCacheInvalidated resets watcher state after discovering the informer
// was removed. Called by get and list when they encounter ErrResourceNotCached.
func (w *Watcher[T]) handleCacheInvalidated() {
	w.mu.Lock()
	w.watching = false
	w.active = false
	w.crdExists = false
	w.generation++
	if w.cancelSyncWaiter != nil {
		w.cancelSyncWaiter()
		w.cancelSyncWaiter = nil
	}
	w.mu.Unlock()
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
			// RemoveInformer is idempotent - safe to call even if
			// waitForSyncAndRequeue already removed it.
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

// defaultSyncTimeout is the default timeout for waiting for the informer
// cache to sync after registering a new watch. This is a safety net for rare
// cases where the informer gets stuck syncing (e.g. misbehaving API server).
// In normal operation sync completes in milliseconds - this just prevents the
// goroutine from blocking for the entire manager lifetime with no signal.
const defaultSyncTimeout = 30 * time.Second

// waitForSyncAndRequeue blocks until the object cache has synced, then
// promotes the watcher to active and enqueues all affected parent objects.
// It is spawned as a goroutine by [Watcher.Ensure] when a new watch is
// registered.
//
// The generation parameter guards against stale promotions: if the CRD is
// removed while this method is blocking, [Watcher.onCRDChange] increments
// the generation and the stale goroutine bails without promoting.
//
//nolint:contextcheck // This goroutine intentionally uses w.ctx (lifecycle) for cleanup/requeue, not syncCtx (timeout-scoped).
func (w *Watcher[T]) waitForSyncAndRequeue(syncCtx context.Context, syncCancel context.CancelFunc, gen uint64, src source.SyncingSource) {
	defer syncCancel() // stop the timeout timer

	log := logf.FromContext(w.ctx)

	syncErr := src.WaitForSync(syncCtx)

	if syncErr != nil {
		w.mu.Lock()
		stale := w.generation != gen
		if !stale {
			w.watching = false
			w.cancelSyncWaiter = nil // goroutine is done
			log.Info("Informer sync failed, will retry on next Ensure()", "crd", w.crdName, "error", syncErr)
		}
		w.mu.Unlock()

		if stale {
			return
		}

		// RemoveInformer is idempotent - safe to call even if onCRDChange already removed it.
		if err := w.objCache.RemoveInformer(w.ctx, w.newT()); err != nil {
			log.Error(err, "Failed to remove informer after sync failure", "crd", w.crdName)
		}

		for _, req := range w.requeueAll(w.ctx) {
			w.queue.Add(req)
		}

		return
	}

	w.mu.Lock()
	if w.generation != gen {
		w.mu.Unlock()

		return
	}

	w.active = true
	w.cancelSyncWaiter = nil // goroutine is done
	w.mu.Unlock()

	log.Info("Dynamic watch ready", "crd", w.crdName)

	for _, req := range w.requeueAll(w.ctx) {
		w.queue.Add(req)
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

func clearList(list client.ObjectList) {
	if err := meta.SetList(list, nil); err != nil {
		items := reflect.ValueOf(list).Elem().FieldByName("Items")
		if items.IsValid() && items.CanSet() {
			items.Set(reflect.MakeSlice(items.Type(), 0, 0))
		}
	}

	list.SetRemainingItemCount(nil)
}
