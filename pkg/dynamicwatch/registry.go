package dynamicwatch

import (
	"fmt"
	"sync"

	ctrl "sigs.k8s.io/controller-runtime"
)

// Registry groups multiple [Watcher] instances behind a single field,
// providing a shared [CRDCache] and type-safe access through [WatchHandle].
//
// Create one per controller via [NewRegistry], register watchers during
// SetupWithManager via [WatcherBuilder.RegisterOn], and query them during
// Reconcile via the returned [WatchHandle] values.
type Registry struct {
	mgr      ctrl.Manager
	crdCache *CRDCache

	mu       sync.RWMutex
	watchers map[string]any // crdName -> *Watcher[T]
}

// NewRegistry creates a Registry with a shared [CRDCache] owned by the
// registry. The CRDCache is registered with the manager and started
// automatically when the manager starts.
func NewRegistry(mgr ctrl.Manager) (*Registry, error) {
	if mgr == nil {
		return nil, fmt.Errorf("dynamicwatch: NewRegistry called with nil manager")
	}

	crdCache, err := NewCRDCache(mgr)
	if err != nil {
		return nil, fmt.Errorf("dynamicwatch: creating shared CRD cache: %w", err)
	}

	return &Registry{
		mgr:      mgr,
		crdCache: crdCache,
		watchers: make(map[string]any),
	}, nil
}

// has returns true if a watcher is already registered for crdName.
func (r *Registry) has(crdName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.watchers[crdName]

	return exists
}

// register stores a watcher in the registry. Returns an error if a watcher
// with the same crdName is already registered.
func (r *Registry) register(crdName string, w any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.watchers[crdName]; exists {
		return fmt.Errorf("dynamicwatch: watcher for %s already registered", crdName)
	}

	r.watchers[crdName] = w

	return nil
}

