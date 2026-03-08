package dynamicwatch_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"
)

// fakeController implements dynamicwatch.WatchRegistrar - the only
// interface the Watcher needs from a controller.
type fakeController struct {
	watchCalls int
	watchErr   error
	mu         sync.Mutex
}

func (f *fakeController) Watch(_ source.TypedSource[reconcile.Request]) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.watchCalls++

	return f.watchErr
}

func (f *fakeController) WatchCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.watchCalls
}

// fakeCache stubs the two cache methods Watcher uses: Get and RemoveInformer.
// The embedded cache.Cache satisfies the interface for source.Kind, which is
// never actually invoked in unit tests (the fakeController absorbs the Watch call).
type fakeCache struct {
	cache.Cache

	removeErr   error
	removeCalls int
	getErr      error
	mu          sync.Mutex
}

func (f *fakeCache) RemoveInformer(_ context.Context, _ client.Object) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removeCalls++

	return f.removeErr
}

func (f *fakeCache) RemoveCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.removeCalls
}

func (f *fakeCache) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return f.getErr
}

const testCRDName = "configmaps.test.io"

func newTestWatcher(fc *fakeCache, ctrl *fakeController) *dynamicwatch.Watcher[*corev1.ConfigMap] {
	return dynamicwatch.NewTestWatcher[*corev1.ConfigMap](testCRDName, fc, ctrl)
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state dynamicwatch.State
		want  string
	}{
		{dynamicwatch.NotAvailable, "NotAvailable"},
		{dynamicwatch.Active, "Active"},
		{dynamicwatch.JustRegistered, "JustRegistered"},
		{dynamicwatch.State(99), "State(99)"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", int(tt.state), got, tt.want)
		}
	}
}

func TestEnsure_BindNotCalled_ReturnsNotAvailable(t *testing.T) {
	w := newTestWatcher(&fakeCache{}, nil)

	state := w.Ensure(t.Context())
	if state != dynamicwatch.NotAvailable {
		t.Errorf("expected NotAvailable without Bind(), got %s", state)
	}
}

func TestEnsure_AlreadyActive_ReturnsActive(t *testing.T) {
	w := newTestWatcher(&fakeCache{}, &fakeController{})
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDAvailable(w, func(_ context.Context) bool {
		t.Fatal("discovery should not be called when already active")

		return false
	})

	state := w.Ensure(t.Context())
	if state != dynamicwatch.Active {
		t.Errorf("expected Active, got %s", state)
	}
}

func TestEnsure_CRDNotAvailable_ReturnsNotAvailable(t *testing.T) {
	w := newTestWatcher(&fakeCache{}, &fakeController{})

	state := w.Ensure(t.Context())
	if state != dynamicwatch.NotAvailable {
		t.Errorf("expected NotAvailable, got %s", state)
	}
}

func TestEnsure_CRDAvailable_WatchSucceeds_ReturnsJustRegistered(t *testing.T) {
	fc := &fakeController{}
	w := newTestWatcher(&fakeCache{}, fc)
	dynamicwatch.SetCRDAvailable(w, func(_ context.Context) bool { return true })

	state := w.Ensure(t.Context())
	if state != dynamicwatch.JustRegistered {
		t.Errorf("expected JustRegistered, got %s", state)
	}

	if fc.WatchCallCount() != 1 {
		t.Errorf("expected 1 Watch call, got %d", fc.WatchCallCount())
	}

	if !w.Available() {
		t.Error("expected Available() to be true after registration")
	}
}

func TestEnsure_CRDAvailable_WatchFails_ReturnsNotAvailable(t *testing.T) {
	fc := &fakeController{watchErr: errors.New("watch failed")}
	w := newTestWatcher(&fakeCache{}, fc)
	dynamicwatch.SetCRDAvailable(w, func(_ context.Context) bool { return true })

	state := w.Ensure(t.Context())
	if state != dynamicwatch.NotAvailable {
		t.Errorf("expected NotAvailable, got %s", state)
	}

	if w.Available() {
		t.Error("expected Available() to be false after failed watch")
	}
}

func TestEnsure_SecondCall_ReturnsActive(t *testing.T) {
	fc := &fakeController{}
	w := newTestWatcher(&fakeCache{}, fc)
	dynamicwatch.SetCRDAvailable(w, func(_ context.Context) bool { return true })

	state := w.Ensure(t.Context())
	if state != dynamicwatch.JustRegistered {
		t.Fatalf("expected JustRegistered on first call, got %s", state)
	}

	state = w.Ensure(t.Context())
	if state != dynamicwatch.Active {
		t.Errorf("expected Active on second call, got %s", state)
	}

	if fc.WatchCallCount() != 1 {
		t.Errorf("expected exactly 1 Watch call across both Ensure calls, got %d", fc.WatchCallCount())
	}
}

func TestGet_Success(t *testing.T) {
	w := newTestWatcher(&fakeCache{}, &fakeController{})
	dynamicwatch.SetActive(w, true)

	err := w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestGet_ErrResourceNotCached_ResetsState(t *testing.T) {
	fc := &fakeCache{getErr: &cache.ErrResourceNotCached{}}
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetActive(w, true)

	err := w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if !errors.Is(err, dynamicwatch.ErrCacheInvalidated) {
		t.Errorf("expected ErrCacheInvalidated, got %v", err)
	}

	if w.Available() {
		t.Error("expected Available() to be false after cache invalidation")
	}
}

func TestGet_OtherError_PreservesState(t *testing.T) {
	someErr := errors.New("something else")
	fc := &fakeCache{getErr: someErr}
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetActive(w, true)

	err := w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if !errors.Is(err, someErr) {
		t.Errorf("expected original error, got %v", err)
	}

	if !w.Available() {
		t.Error("expected Available() to remain true for non-cache errors")
	}
}

func TestSimulateCRDChange_CRDRemoved_CleansUpInformer(t *testing.T) {
	fc := &fakeCache{}
	requeueCalled := false
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		requeueCalled = true

		return []reconcile.Request{{}}
	})

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	requests := dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	if w.Available() {
		t.Error("expected watch to be deactivated after CRD removal")
	}

	if fc.RemoveCallCount() != 1 {
		t.Errorf("expected 1 RemoveInformer call, got %d", fc.RemoveCallCount())
	}

	if !requeueCalled {
		t.Error("expected requeueAll to be called")
	}

	if len(requests) != 1 {
		t.Errorf("expected 1 reconcile request, got %d", len(requests))
	}
}

func TestSimulateCRDChange_RemoveInformerError_StillDeactivates(t *testing.T) {
	fc := &fakeCache{removeErr: errors.New("remove failed")}
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetActive(w, true)

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	if w.Available() {
		t.Error("expected watch to be deactivated even when RemoveInformer fails")
	}

	if fc.RemoveCallCount() != 1 {
		t.Errorf("expected 1 RemoveInformer call, got %d", fc.RemoveCallCount())
	}
}

func TestSimulateCRDChange_CRDNotEstablished_CleansUpInformer(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetActive(w, true)

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionFalse},
			},
		},
	}

	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	if w.Available() {
		t.Error("expected watch to be deactivated when CRD is not established")
	}

	if fc.RemoveCallCount() != 1 {
		t.Errorf("expected 1 RemoveInformer call, got %d", fc.RemoveCallCount())
	}
}

func TestSimulateCRDChange_CRDEstablished_KeepsWatch(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetActive(w, true)

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
			},
		},
	}

	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	if !w.Available() {
		t.Error("expected watch to remain active when CRD is established")
	}

	if fc.RemoveCallCount() != 0 {
		t.Errorf("expected no RemoveInformer calls, got %d", fc.RemoveCallCount())
	}
}

func TestSimulateCRDChange_WatchNotActive_SkipsRemovalAndRequeue(t *testing.T) {
	fc := &fakeCache{}
	requeueCalled := false
	w := newTestWatcher(fc, &fakeController{})
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		requeueCalled = true

		return []reconcile.Request{{}}
	})

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	requests := dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	if fc.RemoveCallCount() != 0 {
		t.Errorf("expected no RemoveInformer calls when watch was inactive, got %d", fc.RemoveCallCount())
	}

	if requeueCalled {
		t.Error("expected requeueAll not to be called when watch was never active")
	}

	if requests != nil {
		t.Errorf("expected nil requests when watch was never active, got %v", requests)
	}
}

func TestEnsure_ConcurrentCalls_RegistersOnce(t *testing.T) {
	fc := &fakeController{}
	w := newTestWatcher(&fakeCache{}, fc)
	dynamicwatch.SetCRDAvailable(w, func(_ context.Context) bool { return true })

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			w.Ensure(t.Context())
		})
	}
	wg.Wait()

	if fc.WatchCallCount() != 1 {
		t.Errorf("expected exactly 1 Watch call from 10 concurrent Ensure calls, got %d", fc.WatchCallCount())
	}
}

func TestBind_PanicsOnNilController(t *testing.T) {
	w := newTestWatcher(&fakeCache{}, nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil controller")
		}
	}()

	w.Bind(nil)
}

func TestBind_PanicsOnDoubleBind(t *testing.T) {
	w := newTestWatcher(&fakeCache{}, nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on double bind")
		}
	}()

	fc := &fakeController{}
	w.Bind(fc)
	w.Bind(fc)
}
