package dynamicwatch

import (
	"errors"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/fields"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// WatcherBuilder constructs a [Watcher] for an optional CRD using a fluent API.
type WatcherBuilder[T client.Object] struct {
	mgr               ctrl.Manager
	crdName           string
	crdCache          *CRDCache
	objHandler        handler.TypedEventHandler[T, reconcile.Request]
	ownerType         client.Object
	ownerOpts         []handler.OwnerOption
	predicates        []predicate.TypedPredicate[T]
	requeueAll        RequeueParentsFn
	defaultNamespaces map[string]cache.Config
}

// For starts building a [Watcher] for an optional CRD.
//
// The type parameter T is the watched resource type (e.g. *PluginConfig).
// Its GVK is derived automatically from the manager's scheme.
//
// The crdName is the fully qualified CRD name (e.g. "pluginconfigs.demo.example.com").
//
// Panics if mgr is nil.
func For[T client.Object](mgr ctrl.Manager, crdName string) *WatcherBuilder[T] {
	if mgr == nil {
		panic("dynamicwatch: For called with nil manager")
	}

	return &WatcherBuilder[T]{
		mgr:     mgr,
		crdName: crdName,
	}
}

// WithCRDCache sets a shared [CRDCache] for CRD lifecycle events. When set,
// the watcher uses the shared cache instead of creating a per-watcher one.
// Events are filtered client-side by CRD name via a predicate.
//
// Panics if c is nil. If not called, [WatcherBuilder.Build] creates a dedicated cache with a
// server-side field selector.
func (b *WatcherBuilder[T]) WithCRDCache(c *CRDCache) *WatcherBuilder[T] {
	if c == nil {
		panic("dynamicwatch: WithCRDCache called with nil CRDCache")
	}

	b.crdCache = c

	return b
}

// WithEventHandler sets the event handler that maps T events
// (create/update/delete) to reconcile requests. This is the general-purpose
// method - the dynamic-watch equivalent of controller-runtime's
// builder.Watches().
//
// Common handlers:
//   - [handler.TypedEnqueueRequestsFromMapFunc] - custom mapping logic
//   - [handler.TypedEnqueueRequestForOwner] - owner-reference-based (see [WatcherBuilder.EnqueueForOwner] for a shortcut)
//
// Mutually exclusive with [WatcherBuilder.EnqueueForOwner].
func (b *WatcherBuilder[T]) WithEventHandler(h handler.TypedEventHandler[T, reconcile.Request]) *WatcherBuilder[T] {
	if h == nil {
		panic("dynamicwatch: WithEventHandler called with nil handler")
	}

	b.objHandler = h

	return b
}

// EnqueueForOwner is a convenience method that configures the watcher to
// enqueue reconcile requests for the owner of the watched object, using
// owner references. This is the dynamic-watch equivalent of
// controller-runtime's builder.Owns().
//
// It is shorthand for calling [WatcherBuilder.WithEventHandler] with
// [handler.TypedEnqueueRequestForOwner].
//
// The ownerType is the type of the owner (e.g. &v1alpha1.Widget{}).
// Optional [handler.OwnerOption] values control matching behavior
// (e.g. handler.OnlyControllerOwner()).
//
// Mutually exclusive with [WatcherBuilder.WithEventHandler].
func (b *WatcherBuilder[T]) EnqueueForOwner(ownerType client.Object, opts ...handler.OwnerOption) *WatcherBuilder[T] {
	if ownerType == nil {
		panic("dynamicwatch: EnqueueForOwner called with nil ownerType")
	}

	b.ownerType = ownerType
	b.ownerOpts = opts

	return b
}

// WithPredicates adds predicates that filter events before they reach the
// event handler. Predicates are passed to [source.Kind] when the watch
// is registered.
func (b *WatcherBuilder[T]) WithPredicates(preds ...predicate.TypedPredicate[T]) *WatcherBuilder[T] {
	b.predicates = append(b.predicates, preds...)

	return b
}

// WithNamespaces restricts the watcher's private object cache to the given
// namespaces. Without this, the cache watches all namespaces - which may
// exceed RBAC permissions in a namespace-scoped deployment and cause
// silent sync failures that look like CRDNotAvailable to callers.
//
// This sets [cache.Options.DefaultNamespaces] on the private cache. CRD
// watches are unaffected (CRDs are cluster-scoped).
func (b *WatcherBuilder[T]) WithNamespaces(namespaces ...string) *WatcherBuilder[T] {
	b.defaultNamespaces = make(map[string]cache.Config, len(namespaces))
	for _, ns := range namespaces {
		b.defaultNamespaces[ns] = cache.Config{}
	}

	return b
}

// EnqueueOnCRDChange sets the function that returns reconcile requests for all
// parent objects affected when the CRD itself is installed or removed.
func (b *WatcherBuilder[T]) EnqueueOnCRDChange(fn RequeueParentsFn) *WatcherBuilder[T] {
	b.requeueAll = fn

	return b
}

// validateCRDName checks if the CRD name is in the valid format <plural>.<group>.
// A valid CRD name must contain at least one dot with non-empty parts on both sides.
func validateCRDName(name string) error {
	if name == "" {
		return errors.New("dynamicwatch: crdName is required")
	}

	// Find the first dot to split plural and group.
	dotIdx := -1
	for i, c := range name {
		if c == '.' {
			dotIdx = i
			break
		}
	}

	// No dot found - bare plural without group.
	if dotIdx == -1 {
		return fmt.Errorf("dynamicwatch: invalid CRD name %q (expected format: <plural>.<group>)", name)
	}

	// Leading dot - empty plural.
	if dotIdx == 0 {
		return fmt.Errorf("dynamicwatch: invalid CRD name %q (expected format: <plural>.<group>)", name)
	}

	// Trailing dot - empty group.
	if dotIdx == len(name)-1 {
		return fmt.Errorf("dynamicwatch: invalid CRD name %q (expected format: <plural>.<group>)", name)
	}

	return nil
}

// Build creates the [Watcher]. Returns an error if required fields are
// missing or if the GVK cannot be derived from the scheme.
func (b *WatcherBuilder[T]) Build() (*Watcher[T], error) {
	if err := validateCRDName(b.crdName); err != nil {
		return nil, err
	}

	hasHandler := b.objHandler != nil
	hasOwner := b.ownerType != nil

	if !hasHandler && !hasOwner {
		return nil, fmt.Errorf("dynamicwatch: WithEventHandler or EnqueueForOwner is required for %s", b.crdName)
	}

	if hasHandler && hasOwner {
		return nil, fmt.Errorf("dynamicwatch: WithEventHandler and EnqueueForOwner are mutually exclusive for %s", b.crdName)
	}

	if b.requeueAll == nil {
		return nil, fmt.Errorf("dynamicwatch: EnqueueOnCRDChange is required for %s", b.crdName)
	}

	newT := newInstance[T]

	_, err := apiutil.GVKForObject(newT(), b.mgr.GetScheme())
	if err != nil {
		return nil, fmt.Errorf("deriving GVK for %s: %w", b.crdName, err)
	}

	var crdCache cache.Cache
	if b.crdCache != nil {
		crdCache = b.crdCache.cache
	} else {
		// Per-watcher cache with server-side field selector.
		c, cacheErr := cache.New(b.mgr.GetConfig(), cache.Options{
			HTTPClient: b.mgr.GetHTTPClient(),
			Scheme:     b.mgr.GetScheme(),
			Mapper:     b.mgr.GetRESTMapper(),
			ByObject: map[client.Object]cache.ByObject{
				&apiextensionsv1.CustomResourceDefinition{}: {
					Field:     fields.OneTermEqualSelector("metadata.name", b.crdName),
					Transform: transformCRDForCache,
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
	//
	// Note: we cannot scope this cache with ByObject because the CRD for type T
	// may not be installed yet. ByObject resolves namespace scoping via the REST
	// mapper at creation time, which fails for unregistered GVKs.
	// ReaderFailOnMissingInformer prevents accidental informer creation instead.
	objCache, err := cache.New(b.mgr.GetConfig(), cache.Options{
		HTTPClient:                  b.mgr.GetHTTPClient(),
		Scheme:                      b.mgr.GetScheme(),
		Mapper:                      b.mgr.GetRESTMapper(),
		ReaderFailOnMissingInformer: true,
		DefaultNamespaces:           b.defaultNamespaces,
	})
	if err != nil {
		return nil, fmt.Errorf("creating object cache for %s: %w", b.crdName, err)
	}

	if addErr := b.mgr.Add(objCache); addErr != nil {
		return nil, fmt.Errorf("registering object cache for %s: %w", b.crdName, addErr)
	}

	objHandler := b.objHandler
	if hasOwner {
		if _, ownerErr := apiutil.GVKForObject(b.ownerType, b.mgr.GetScheme()); ownerErr != nil {
			return nil, fmt.Errorf("deriving GVK for owner type of %s: %w", b.crdName, ownerErr)
		}

		objHandler = handler.TypedEnqueueRequestForOwner[T](
			b.mgr.GetScheme(), b.mgr.GetRESTMapper(), b.ownerType, b.ownerOpts...)
	}

	w := &Watcher[T]{
		crdName:    b.crdName,
		objCache:   objCache,
		crdCache:   crdCache,
		objHandler: objHandler,
		predicates: b.predicates,
		requeueAll: b.requeueAll,
		newT:       newT,
	}

	return w, nil
}
