package dynamicwatch_test

import (
	"context"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"
)

func TestRegistry_DuplicateCRDName(t *testing.T) {
	reg := dynamicwatch.NewTestRegistry()
	w := dynamicwatch.NewTestWatcher[client.Object]("widgets.example.com", &fakeCache{}, context.Background())

	if err := dynamicwatch.RegistryRegister(reg, "widgets.example.com", w); err != nil {
		t.Fatalf("first register should succeed: %v", err)
	}

	w2 := dynamicwatch.NewTestWatcher[client.Object]("widgets.example.com", &fakeCache{}, context.Background())
	if err := dynamicwatch.RegistryRegister(reg, "widgets.example.com", w2); err == nil {
		t.Fatal("expected error on duplicate registration, got nil")
	}
}

func TestHandle_Available_WhenActive(t *testing.T) {
	fc := &fakeCache{}
	ctx := context.Background()
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", fc, ctx)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	h := dynamicwatch.NewTestHandle(w)

	if !h.Available() {
		t.Error("expected Available() to return true when watcher is active")
	}
}

func TestHandle_Available_WhenInactive(t *testing.T) {
	fc := &fakeCache{}
	ctx := context.Background()
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", fc, ctx)

	h := dynamicwatch.NewTestHandle(w)

	if h.Available() {
		t.Error("expected Available() to return false when watcher is not active")
	}
}

func TestHandle_Status_ReflectsWatcherState(t *testing.T) {
	fc := &fakeCache{}
	ctx := context.Background()
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", fc, ctx)

	h := dynamicwatch.NewTestHandle(w)

	s := h.Status()
	if s.Available {
		t.Error("expected Status.Available=false for inactive watcher")
	}
	if s.Reason != dynamicwatch.ReasonCRDNotFound {
		t.Errorf("expected reason CRDNotFound, got %s", s.Reason)
	}

	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	s = h.Status()
	if !s.Available {
		t.Error("expected Status.Available=true after activation")
	}
	if s.Reason != dynamicwatch.ReasonReady {
		t.Errorf("expected reason Ready, got %s", s.Reason)
	}
}

func TestHandle_TryGet_DelegatesToWatcher(t *testing.T) {
	fc := &fakeCache{}
	ctx := context.Background()
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", fc, ctx)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	h := dynamicwatch.NewTestHandle(w)

	cm := &corev1.ConfigMap{}
	ok, err := h.TryGet(ctx, client.ObjectKey{Name: "test", Namespace: "default"}, cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected TryGet to return true when watcher is active")
	}
}

func TestHandle_TryGet_ReturnsFalse_WhenCRDUnavailable(t *testing.T) {
	fc := &fakeCache{}
	ctx := context.Background()
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", fc, ctx)

	h := dynamicwatch.NewTestHandle(w)

	cm := &corev1.ConfigMap{}
	ok, err := h.TryGet(ctx, client.ObjectKey{Name: "test"}, cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected TryGet to return false when CRD is unavailable")
	}
}

func TestHandle_ConcurrentReads(t *testing.T) {
	fc := &fakeCache{}
	ctx := context.Background()
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", fc, ctx)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	h := dynamicwatch.NewTestHandle(w)

	var wg sync.WaitGroup
	const goroutines = 50

	for range goroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_ = h.Available()
			_ = h.Status()
		}()
	}

	wg.Wait()
}

func TestHandle_MultipleTypes(t *testing.T) {
	ctx := context.Background()

	cmWatcher := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("configmaps.example.com", &fakeCache{}, ctx)
	dynamicwatch.SetCRDExists(cmWatcher, true)
	dynamicwatch.SetActive(cmWatcher, true)

	secretWatcher := dynamicwatch.NewTestWatcher[*corev1.Secret]("secrets.example.com", &fakeCache{}, ctx)

	cmHandle := dynamicwatch.NewTestHandle(cmWatcher)
	secretHandle := dynamicwatch.NewTestHandle(secretWatcher)

	if !cmHandle.Available() {
		t.Error("expected ConfigMap handle to be available")
	}
	if secretHandle.Available() {
		t.Error("expected Secret handle to not be available")
	}
}
