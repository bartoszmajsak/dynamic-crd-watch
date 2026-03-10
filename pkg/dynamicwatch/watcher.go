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
//	    EnqueueOnObjectChange(r.pluginConfigToWidgets).
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

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// State represents the result of a [Watcher.Ensure] call.
type State int

const (
	// Unavailable means the CRD is not installed or the watch could not be registered.
	Unavailable State = iota
	// Ready means the watch is running and the informer cache has synced.
	// It is safe to read from the cache immediately.
	Ready
	// Syncing means the watch was registered but the informer has not
	// completed its initial list yet. The caller should requeue - no
	// arbitrary delay is needed since the next reconcile will re-check
	// [cache.Informer.HasSynced] and transition to [Ready] as soon as
	// the cache is populated.
	Syncing
)

func (s State) String() string {
	switch s {
	case Unavailable:
		return "Unavailable"
	case Ready:
		return "Ready"
	case Syncing:
		return "Syncing"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

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
				Transform: stripCRDSpec,
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

// WatcherBuilder constructs a [Watcher] for an optional CRD using a fluent API.
type WatcherBuilder[T client.Object] struct {
	mgr          ctrl.Manager
	crdName      string
	crdCache     *CRDCache
	objectMapper handler.TypedMapFunc[T, reconcile.Request]
	requeueAll   RequeueParentsFn
}

// For starts building a [Watcher] for an optional CRD.
//
// The type parameter T is the watched resource type (e.g. *PluginConfig).
// Its GVK is derived automatically from the manager's scheme.
//
// The crdName is the fully qualified CRD name (e.g. "pluginconfigs.demo.example.com").
func For[T client.Object](mgr ctrl.Manager, crdName string) *WatcherBuilder[T] {
	return &WatcherBuilder[T]{
		mgr:     mgr,
		crdName: crdName,
	}
}

// WithCRDCache sets a shared [CRDCache] for CRD lifecycle events. When set,
// the watcher uses the shared cache instead of creating a per-watcher one.
// Events are filtered client-side by CRD name via a predicate.
//
// Panics if c is nil - use a nil-safe constructor or omit the call entirely
// to get the per-watcher cache fallback.
//
// If not called, [WatcherBuilder.Build] creates a dedicated cache with a
// server-side field selector for backward compatibility.
func (b *WatcherBuilder[T]) WithCRDCache(c *CRDCache) *WatcherBuilder[T] {
	if c == nil {
		panic("dynamicwatch: WithCRDCache called with nil CRDCache")
	}

	b.crdCache = c

	return b
}

// EnqueueOnObjectChange sets the function that maps individual T events
// (create/update/delete) to reconcile requests for parent objects.
func (b *WatcherBuilder[T]) EnqueueOnObjectChange(fn handler.TypedMapFunc[T, reconcile.Request]) *WatcherBuilder[T] {
	b.objectMapper = fn

	return b
}

// EnqueueOnCRDChange sets the function that returns reconcile requests for all
// parent objects affected when the CRD itself is installed or removed.
func (b *WatcherBuilder[T]) EnqueueOnCRDChange(fn RequeueParentsFn) *WatcherBuilder[T] {
	b.requeueAll = fn

	return b
}

// Build creates the [Watcher]. Returns an error if required fields are
// missing or if the GVK cannot be derived from the scheme.
func (b *WatcherBuilder[T]) Build() (*Watcher[T], error) {
	if b.mgr == nil {
		return nil, errors.New("dynamicwatch: manager is required")
	}

	if b.crdName == "" {
		return nil, errors.New("dynamicwatch: crdName is required")
	}

	if b.objectMapper == nil {
		return nil, fmt.Errorf("dynamicwatch: EnqueueOnObjectChange is required for %s", b.crdName)
	}

	if b.requeueAll == nil {
		return nil, fmt.Errorf("dynamicwatch: EnqueueOnCRDChange is required for %s", b.crdName)
	}

	newT := newInstance[T]

	gvk, err := apiutil.GVKForObject(newT(), b.mgr.GetScheme())
	if err != nil {
		return nil, fmt.Errorf("deriving GVK for %s: %w", b.crdName, err)
	}

	var crdCache cache.Cache
	if b.crdCache != nil {
		crdCache = b.crdCache.cache
	} else {
		// Per-watcher cache with server-side field selector (backward compat).
		c, cacheErr := cache.New(b.mgr.GetConfig(), cache.Options{
			HTTPClient: b.mgr.GetHTTPClient(),
			Scheme:     b.mgr.GetScheme(),
			Mapper:     b.mgr.GetRESTMapper(),
			ByObject: map[client.Object]cache.ByObject{
				&apiextensionsv1.CustomResourceDefinition{}: {
					Field: fields.OneTermEqualSelector("metadata.name", b.crdName),
				},
			},
		})
		if cacheErr != nil {
			return nil, fmt.Errorf("creating CRD cache for %s: %w", b.crdName, cacheErr)
		}

		if addErr := b.mgr.Add(c); addErr != nil {
			return nil, fmt.Errorf("registering CRD cache for %s: %w", b.crdName, addErr)
		}

		crdCache = c
	}

	// Private cache for object type T. Each Watcher gets its own cache so that
	// RemoveInformer only affects this Watcher - not other controllers in the
	// same manager that may watch the same GVK.
	objCache, err := cache.New(b.mgr.GetConfig(), cache.Options{
		HTTPClient:                  b.mgr.GetHTTPClient(),
		Scheme:                      b.mgr.GetScheme(),
		Mapper:                      b.mgr.GetRESTMapper(),
		ReaderFailOnMissingInformer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating object cache for %s: %w", b.crdName, err)
	}

	if addErr := b.mgr.Add(objCache); addErr != nil {
		return nil, fmt.Errorf("registering object cache for %s: %w", b.crdName, addErr)
	}

	w := &Watcher[T]{
		crdName:      b.crdName,
		gvk:          gvk,
		objCache:     objCache,
		crdCache:     crdCache,
		objectMapper: b.objectMapper,
		requeueAll:   b.requeueAll,
		newT:         newT,
	}

	return w, nil
}

// Compile-time check: Watcher must satisfy source.SyncingSource.
var _ source.SyncingSource = (*Watcher[client.Object])(nil)

// Watcher manages the lifecycle of a watch for an optional CRD of type T.
// It implements [source.SyncingSource] so it can be passed directly to
// [builder.Builder.WatchesRawSource].
type Watcher[T client.Object] struct {
	crdName      string
	gvk          schema.GroupVersionKind
	objCache     cache.Cache
	crdCache     cache.Cache
	objectMapper handler.TypedMapFunc[T, reconcile.Request]
	requeueAll   RequeueParentsFn
	newT         func() T

	// startSource is set by Start. It starts a sub-source using the
	// controller's lifecycle context and queue, both captured at Start time.
	startSource func(src source.SyncingSource) error
	crdSrc      source.SyncingSource

	mu        sync.Mutex
	watching  bool // source registered via Watch (may not be synced yet)
	active    bool // informer synced - safe to read
	crdExists bool
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
// Registration is a two-phase process: first, the watch source is
// started directly using the queue from [Watcher.Start] (non-blocking).
// Then, on this or a subsequent call, Ensure checks whether the informer
// has completed its initial list via [cache.Informer.HasSynced]. Only
// after sync does Ensure return [Ready]. This avoids blocking the
// reconcile worker and prevents deadlocks with [Watcher.onCRDChange].
//
// The caller does not need to distinguish "just registered" from
// "already synced" - if the informer hasn't synced yet, Ensure returns
// [Syncing] and the caller should requeue after a short delay.
func (w *Watcher[T]) Ensure(ctx context.Context) State {
	if w.startSource == nil {
		logf.FromContext(ctx).Error(nil, "Start() not called, cannot register watch", "crd", w.crdName)

		return Unavailable
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active {
		return Ready
	}

	if !w.crdExists {
		return Unavailable
	}

	log := logf.FromContext(ctx)

	if !w.watching {
		src := source.Kind(w.objCache, w.newT(),
			handler.TypedEnqueueRequestsFromMapFunc(w.objectMapper),
		)

		if err := w.startSource(src); err != nil {
			log.Error(err, "Failed to start dynamic watch", "crd", w.crdName)

			return Unavailable
		}

		w.watching = true
		log.Info("Started dynamic watch, waiting for informer sync", "crd", w.crdName)
	}

	informer, err := w.objCache.GetInformer(ctx, w.newT(), cache.BlockUntilSynced(false))
	if err != nil {
		log.Error(err, "Failed to get informer", "crd", w.crdName)

		return Unavailable
	}

	if !informer.HasSynced() {
		return Syncing
	}

	w.active = true
	log.Info("Dynamic watch ready", "crd", w.crdName)

	return Ready
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

// crdNamePredicate returns a predicate that matches CRDs by metadata.name.
// Used to filter shared CRD cache events to the specific CRD this watcher
// is tracking.
func crdNamePredicate(name string) predicate.TypedPredicate[*apiextensionsv1.CustomResourceDefinition] {
	return predicate.NewTypedPredicateFuncs(func(crd *apiextensionsv1.CustomResourceDefinition) bool {
		return crd.Name == name
	})
}

// stripCRDSpec is a cache transform that removes the spec from CRD objects
// before they are stored in the informer's in-memory store. CRD specs contain
// large OpenAPI schemas (validation, versions) that we never read - we only
// need metadata (name, deletionTimestamp) and status (Established condition).
func stripCRDSpec(obj any) (any, error) {
	if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
		crd.Spec = apiextensionsv1.CustomResourceDefinitionSpec{}
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
