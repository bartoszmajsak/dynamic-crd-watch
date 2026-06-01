package dynamicwatch

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WatchHandle is a typed, ergonomic accessor for a [Watcher] stored in a
// [Registry]. It captures a direct pointer to the watcher so the reconcile
// hot path avoids map lookups and lock acquisition on the registry.
//
// WatchHandle is a value type - cheap to copy and safe to store as a struct
// field on a reconciler. It is obtained from [WatcherBuilder.RegisterOn].
type WatchHandle[T client.Object] struct {
	w *Watcher[T]
}

// Ensure checks CRD availability and registers the watch if needed.
// See [Watcher.Ensure] for full semantics.
func (h WatchHandle[T]) Ensure(ctx context.Context) bool {
	return h.w.Ensure(ctx)
}

// TryGet combines Ensure and a cache read. See [Watcher.TryGet].
func (h WatchHandle[T]) TryGet(ctx context.Context, key client.ObjectKey, obj T) (bool, error) {
	return h.w.TryGet(ctx, key, obj)
}

// TryList combines Ensure and a cache list. See [Watcher.TryList].
func (h WatchHandle[T]) TryList(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (bool, error) {
	return h.w.TryList(ctx, list, opts...)
}

// Available reports whether the watch is currently active.
// See [Watcher.Available].
func (h WatchHandle[T]) Available() bool {
	return h.w.Available()
}

// Status returns diagnostic information about the watcher's current state.
// See [Watcher.Status].
func (h WatchHandle[T]) Status() WatcherStatus {
	return h.w.Status()
}

// Condition returns a [metav1.Condition] reflecting the watcher's current
// state. See [Watcher.Condition].
func (h WatchHandle[T]) Condition() metav1.Condition {
	return h.w.Condition()
}
