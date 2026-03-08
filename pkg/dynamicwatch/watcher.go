// Package dynamicwatch manages watches for optional CRDs that may be
// installed or removed at runtime. Each [Watcher] tracks one CRD's lifecycle -
// registering an informer when the CRD appears and tearing it down when it
// disappears - without requiring a controller restart.
//
// The cache must be created with [cache.Options.ReaderFailOnMissingInformer]
// set to true. Without it, a read after informer removal silently starts a
// new informer that blocks on sync forever.
//
// Usage follows a fluent builder pattern familiar from controller-runtime:
//
//	w, err := dynamicwatch.For[*v1alpha1.PluginConfig](mgr, "pluginconfigs.demo.example.com").
//	    EnqueueOnObjectChange(r.pluginConfigToWidgets).
//	    EnqueueOnCRDChange(r.allWidgetsWithPluginRef).
//	    Build()
//
//	w.Register(b)   // before builder.Build()
//	c, _ := b.Build(r)
//	w.Bind(c)       // after builder.Build()
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
	"k8s.io/client-go/discovery"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// State represents the result of a [Watcher.Ensure] call.
type State int

const (
	// NotAvailable means the CRD is not installed or the watch could not be registered.
	NotAvailable State = iota
	// Active means the watch was already running before this call.
	Active
	// JustRegistered means the watch was registered during this call.
	// The informer cache may not have synced yet - callers should requeue
	// rather than reading immediately.
	JustRegistered
)

func (s State) String() string {
	switch s {
	case NotAvailable:
		return "NotAvailable"
	case Active:
		return "Active"
	case JustRegistered:
		return "JustRegistered"
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

// WatchRegistrar is the subset of [controller.Controller] that a [Watcher]
// needs to register dynamic watches at runtime. Accepting this narrow
// interface instead of the full controller makes the dependency explicit
// and simplifies test doubles.
type WatchRegistrar interface {
	Watch(src source.TypedSource[reconcile.Request]) error
}

// WatcherBuilder constructs a [Watcher] for an optional CRD using a fluent API.
type WatcherBuilder[T client.Object] struct {
	mgr          ctrl.Manager
	crdName      string
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

	dc, err := discovery.NewDiscoveryClientForConfigAndClient(b.mgr.GetConfig(), b.mgr.GetHTTPClient())
	if err != nil {
		return nil, fmt.Errorf("creating discovery client for %s: %w", b.crdName, err)
	}

	crdCache, err := cache.New(b.mgr.GetConfig(), cache.Options{
		HTTPClient: b.mgr.GetHTTPClient(),
		Scheme:     b.mgr.GetScheme(),
		Mapper:     b.mgr.GetRESTMapper(),
		ByObject: map[client.Object]cache.ByObject{
			&apiextensionsv1.CustomResourceDefinition{}: {
				Field: fields.OneTermEqualSelector("metadata.name", b.crdName),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating CRD cache for %s: %w", b.crdName, err)
	}

	if err := b.mgr.Add(crdCache); err != nil {
		return nil, fmt.Errorf("registering CRD cache for %s: %w", b.crdName, err)
	}

	w := &Watcher[T]{
		crdName:      b.crdName,
		gvk:          gvk,
		cache:        b.mgr.GetCache(),
		crdCache:     crdCache,
		objectMapper: b.objectMapper,
		requeueAll:   b.requeueAll,
		newT:         newT,
	}

	w.crdAvailable = crdChecker(dc, gvk)

	return w, nil
}

// Watcher manages the lifecycle of a watch for an optional CRD of type T.
// Create one via [For], wire it into the controller builder via
// [Watcher.Register], and connect it to the controller via [Watcher.Bind].
type Watcher[T client.Object] struct {
	crdName      string
	gvk          schema.GroupVersionKind
	cache        cache.Cache
	crdCache     cache.Cache
	ctrl         WatchRegistrar
	crdAvailable func(ctx context.Context) bool
	objectMapper handler.TypedMapFunc[T, reconcile.Request]
	requeueAll   RequeueParentsFn
	newT         func() T
	mu           sync.Mutex
	active       bool
}

// Register adds a CRD watch to the controller builder using the watcher's
// dedicated cache. Each Watcher gets its own cache filtered by metadata.name
// via a server-side field selector, so multiple Watchers can independently
// track different CRDs without interfering with each other.
// Must be called before [builder.Builder.Build].
func (w *Watcher[T]) Register(b *builder.Builder) {
	b.WatchesRawSource(source.Kind(w.crdCache, &apiextensionsv1.CustomResourceDefinition{},
		handler.TypedEnqueueRequestsFromMapFunc(w.onCRDChange),
	))
}

// Bind connects the watcher to the built controller, enabling dynamic
// watch registration at runtime. Must be called after [builder.Builder.Build].
// Accepts any [WatchRegistrar] - typically the [controller.Controller]
// returned by the builder.
//
// Bind panics on nil or double-bind because these are programming errors
// in controller setup, not recoverable runtime conditions. This matches
// the Go stdlib convention for setup-time invariants (see [regexp.MustCompile],
// [template.Must]) and controller-runtime's own builder which panics on
// duplicate For() calls.
func (w *Watcher[T]) Bind(ctrl WatchRegistrar) {
	if ctrl == nil {
		panic("dynamicwatch: Bind called with nil controller")
	}

	if w.ctrl != nil {
		panic("dynamicwatch: Bind called twice")
	}

	w.ctrl = ctrl
}

// Ensure checks CRD availability and registers the watch if needed.
// Uses a check-lock-check pattern to avoid holding the mutex during
// the discovery API call.
//
// The mutex is held during [WatchRegistrar.Watch] to prevent double
// registration from concurrent reconciles. This is safe because
// controller-runtime's Watch is non-blocking (it adds a source to
// the controller's internal list). If a custom WatchRegistrar blocks,
// it will delay concurrent [Watcher.Get], [Watcher.Available], and
// [Watcher.onCRDChange] calls until registration completes.
//
// There is an inherent TOCTOU window between the discovery check and
// watch registration - the CRD could be removed in that interval.
// This is safe because [Watcher.onCRDChange] will fire for the removal,
// clean up the informer via [cache.Cache.RemoveInformer], and reset
// the active flag. The next reconcile will see [NotAvailable].
func (w *Watcher[T]) Ensure(ctx context.Context) State {
	if w.ctrl == nil {
		logf.FromContext(ctx).Error(nil, "Bind() not called, cannot register watch", "crd", w.crdName)

		return NotAvailable
	}

	w.mu.Lock()
	if w.active {
		w.mu.Unlock()

		return Active
	}
	w.mu.Unlock()

	if !w.crdAvailable(ctx) {
		return NotAvailable
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active {
		return Active
	}

	log := logf.FromContext(ctx)

	src := source.Kind(w.cache, w.newT(),
		handler.TypedEnqueueRequestsFromMapFunc(w.objectMapper),
	)

	if err := w.ctrl.Watch(src); err != nil {
		log.Error(err, "Failed to register dynamic watch", "crd", w.crdName)

		return NotAvailable
	}

	w.active = true
	log.Info("Dynamically registered watch", "crd", w.crdName)

	return JustRegistered
}

// Get reads an object of type T from the cache. If the informer was removed
// between [Watcher.Ensure] and this call (the RemoveInformer/Get race), the
// watcher resets its internal state and returns [ErrCacheInvalidated]. The
// caller should requeue.
//
// All other errors (including NotFound) are returned as-is.
func (w *Watcher[T]) Get(ctx context.Context, key client.ObjectKey, obj T) error {
	if err := w.cache.Get(ctx, key, obj); err != nil {
		var notCached *cache.ErrResourceNotCached
		if errors.As(err, &notCached) {
			w.mu.Lock()
			w.active = false
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
// unnecessary reconciles that hit Ensure() → NotAvailable.
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
		wasActive := w.active
		w.active = false
		w.mu.Unlock()

		if wasActive {
			if err := w.cache.RemoveInformer(ctx, w.newT()); err != nil {
				log.Error(err, "Failed to remove informer", "crd", w.crdName)
			}

			log.Info("Removed watch after CRD deletion", "crd", w.crdName)
		}

		// Only requeue if the watch was active - if it was never registered,
		// there are no cached objects to invalidate and no state to re-evaluate.
		if !wasActive {
			return nil
		}
	} else {
		log.Info("CRD detected, will requeue affected objects", "crd", w.crdName)
	}

	return w.requeueAll(ctx)
}

// crdChecker returns a function that checks CRD availability via the
// discovery API. Uses an uncached client to avoid the ~10min TTL of
// the cached discovery client.
func crdChecker(dc *discovery.DiscoveryClient, gvk schema.GroupVersionKind) func(ctx context.Context) bool {
	return func(ctx context.Context) bool {
		resources, err := dc.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
		if err != nil {
			logf.FromContext(ctx).V(1).Info("API group not available via discovery",
				"groupVersion", gvk.GroupVersion().String(), "error", err)

			return false
		}

		for i := range resources.APIResources {
			if resources.APIResources[i].Kind == gvk.Kind {
				return true
			}
		}

		return false
	}
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
