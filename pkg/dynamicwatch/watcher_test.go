package dynamicwatch_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"
)

// fakeCache stubs the cache methods Watcher uses: Get, List, RemoveInformer,
// GetInformer, and WaitForCacheSync.
type fakeCache struct {
	cache.Cache

	removeErr        error
	removeCalls      int
	getErr           error
	listErr          error
	getInformerErr   error
	getInformerCalls int
	mu               sync.Mutex
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

func (f *fakeCache) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if f.listErr != nil {
		return f.listErr
	}

	// Set empty items on successful list
	if cmList, ok := list.(*corev1.ConfigMapList); ok {
		cmList.Items = []corev1.ConfigMap{}
	}

	return nil
}

func (f *fakeCache) GetInformer(_ context.Context, _ client.Object, _ ...cache.InformerGetOption) (cache.Informer, error) {
	f.mu.Lock()
	f.getInformerCalls++
	f.mu.Unlock()

	if f.getInformerErr != nil {
		return nil, f.getInformerErr
	}

	return &fakeInformer{}, nil
}

func (f *fakeCache) GetInformerCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.getInformerCalls
}

func (f *fakeCache) WaitForCacheSync(_ context.Context) bool {
	return true
}

// fakeInformer satisfies cache.Informer for source.Kind.Start.
type fakeInformer struct {
	cache.Informer
}

func (f *fakeInformer) AddEventHandler(_ toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return &fakeRegistration{}, nil
}

func (f *fakeInformer) AddEventHandlerWithOptions(_ toolscache.ResourceEventHandler, _ toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	return &fakeRegistration{}, nil
}

func (f *fakeInformer) HasSynced() bool {
	return true
}

// fakeRegistration satisfies toolscache.ResourceEventHandlerRegistration.
type fakeRegistration struct{}

func (f *fakeRegistration) HasSynced() bool {
	return true
}

// fakeSyncingSource is used with CallWaitForSyncAndRequeue to test the
// real waitForSyncAndRequeue code path.
type fakeSyncingSource struct {
	source.SyncingSource

	syncErr error
}

func (f *fakeSyncingSource) WaitForSync(_ context.Context) error {
	return f.syncErr
}

const testCRDName = "configmaps.test.io"

func newTestWatcher(fc *fakeCache) *dynamicwatch.Watcher[*corev1.ConfigMap] {
	return dynamicwatch.NewTestWatcher[*corev1.ConfigMap](testCRDName, fc, context.Background())
}

func newStartedWatcher(fc *fakeCache) *dynamicwatch.Watcher[*corev1.ConfigMap] {
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))

	return w
}

func TestWaitForSync_BeforeStart_ReturnsError(t *testing.T) {
	w := dynamicwatch.NewUnstartedTestWatcher[*corev1.ConfigMap](testCRDName, &fakeCache{})

	err := w.WaitForSync(t.Context())
	if err == nil {
		t.Error("expected error when WaitForSync called before Start")
	}
}

func TestEnsure_BeforeStart_Panics(t *testing.T) {
	w := dynamicwatch.NewUnstartedTestWatcher[*corev1.ConfigMap](testCRDName, &fakeCache{})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on Ensure before Start")
		}
	}()

	w.Ensure(t.Context())
}

func TestEnsure_CRDUnavailable_ReturnsFalse(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})

	if w.Ensure(t.Context()) {
		t.Error("expected false when CRD not available")
	}
}

func TestEnsure_CRDAvailable_StartsWatch_ReturnsFalse(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetCRDExists(w, true)

	// First Ensure starts the watch but returns false - the sync waiter
	// goroutine handles promotion to active.
	if w.Ensure(t.Context()) {
		t.Error("expected false on first Ensure (sync waiter pending)")
	}

	if w.Available() {
		t.Error("expected Available() to be false before sync waiter promotes")
	}
}

func TestEnsure_AlreadyActive_ReturnsTrue(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	if !w.Ensure(t.Context()) {
		t.Error("expected true when already active")
	}

	if !w.Available() {
		t.Error("expected Available() to be true")
	}
}

func TestTryGet_CRDAvailable_ObjectFound(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetActive(w, true)

	available, err := w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !available {
		t.Error("expected available=true")
	}
}

func TestTryGet_CRDUnavailable_ReturnsFalseNil(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})

	available, err := w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if available {
		t.Error("expected available=false when CRD not available")
	}

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestTryGet_ErrResourceNotCached_ReturnsFalseNil(t *testing.T) {
	fc := &fakeCache{getErr: &cache.ErrResourceNotCached{}}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)

	available, err := w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if available {
		t.Error("expected available=false after cache invalidation")
	}

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	if w.Available() {
		t.Error("expected Available() to be false after cache invalidation")
	}

	// Verify crdExists was also reset - Ensure should return false.
	if w.Ensure(t.Context()) {
		t.Error("expected false after cache invalidation")
	}
}

func TestTryGet_OtherError_ReturnsTrueWithError(t *testing.T) {
	someErr := errors.New("something else")
	fc := &fakeCache{getErr: someErr}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)

	available, err := w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if !available {
		t.Error("expected available=true for non-cache errors")
	}

	if !errors.Is(err, someErr) {
		t.Errorf("expected original error, got %v", err)
	}

	if !w.Available() {
		t.Error("expected Available() to remain true for non-cache errors")
	}
}

func TestTryGet_NotFound_ReturnsTrueWithError(t *testing.T) {
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{}, "test")
	fc := &fakeCache{getErr: notFoundErr}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)

	available, err := w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if !available {
		t.Error("expected available=true for NotFound")
	}

	if !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got %v", err)
	}
}

func TestTryGet_ErrResourceNotCached_IncrementsGeneration(t *testing.T) {
	fc := &fakeCache{getErr: &cache.ErrResourceNotCached{}}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)

	genBefore := dynamicwatch.Generation(w)

	_, _ = w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	genAfter := dynamicwatch.Generation(w)
	if genAfter != genBefore+1 {
		t.Errorf("expected generation to increment from %d to %d, got %d", genBefore, genBefore+1, genAfter)
	}
}

func TestTryList_CRDAvailable_ReturnsItems(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetActive(w, true)

	list := &corev1.ConfigMapList{}
	available, err := w.TryList(t.Context(), list)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !available {
		t.Error("expected available=true")
	}
}

func TestTryList_CRDUnavailable_ReturnsFalseNilAndClearsList(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	// Don't set CRDExists - CRD unavailable

	list := &corev1.ConfigMapList{
		Items: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "stale"}}},
	}
	available, err := w.TryList(t.Context(), list)

	if available {
		t.Error("expected available=false when CRD not available")
	}

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	if len(list.Items) != 0 {
		t.Errorf("expected list Items to be cleared, got %d items", len(list.Items))
	}
}

func TestTryList_ErrResourceNotCached_ReturnsFalseNilAndClearsList(t *testing.T) {
	fc := &fakeCache{listErr: &cache.ErrResourceNotCached{}}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)

	list := &corev1.ConfigMapList{
		Items: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "stale"}}},
	}
	available, err := w.TryList(t.Context(), list)

	if available {
		t.Error("expected available=false after cache invalidation")
	}

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	if len(list.Items) != 0 {
		t.Errorf("expected list Items to be cleared, got %d items", len(list.Items))
	}
}

func TestSimulateCRDChange_CRDRemoved_CleansUpInformer(t *testing.T) {
	fc := &fakeCache{}
	requeueCalled := false
	w := newStartedWatcher(fc)
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
	w := newStartedWatcher(fc)
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
	w := newStartedWatcher(fc)
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
	w := newStartedWatcher(fc)
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

func TestSimulateCRDChange_CRDAlreadyKnown_SkipsRequeue(t *testing.T) {
	fc := &fakeCache{}
	requeueCount := 0
	w := newStartedWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		requeueCount++

		return []reconcile.Request{{}}
	})

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
			},
		},
	}

	// Duplicate CRD event when already known - should not requeue.
	requests := dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	if requeueCount != 0 {
		t.Errorf("expected no requeue calls for already-known CRD, got %d", requeueCount)
	}

	if requests != nil {
		t.Errorf("expected nil requests for already-known CRD, got %v", requests)
	}

	if !w.Available() {
		t.Error("expected watch to remain active")
	}
}

func TestSimulateCRDChange_WatchNotActive_SkipsRemovalAndRequeue(t *testing.T) {
	fc := &fakeCache{}
	requeueCalled := false
	w := newStartedWatcher(fc)
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

func TestSimulateCRDChange_WatchingSyncingNotActive_RemovesInformerSkipsRequeue(t *testing.T) {
	fc := &fakeCache{}
	requeueCalled := false
	w := newStartedWatcher(fc)
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetCRDExists(w, true)
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

	if fc.RemoveCallCount() != 1 {
		t.Errorf("expected 1 RemoveInformer call for syncing watch, got %d", fc.RemoveCallCount())
	}

	if requeueCalled {
		t.Error("expected requeueAll not to be called when watch was syncing (not active)")
	}

	if requests != nil {
		t.Errorf("expected nil requests when watch was syncing, got %v", requests)
	}

	if w.Available() {
		t.Error("expected Available() to be false after CRD removal")
	}
}

func TestEnsure_SourceStartFails_ReturnsFalse(t *testing.T) {
	fc := &fakeCache{getInformerErr: errors.New("start failed")}
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))
	dynamicwatch.SetCRDExists(w, true)

	if w.Ensure(t.Context()) {
		t.Error("expected false on start failure")
	}
}

func TestEnsure_ConcurrentCalls_RegistersOnce(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))
	dynamicwatch.SetCRDExists(w, true)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			w.Ensure(t.Context())
		})
	}
	wg.Wait()

	// Give the goroutine time to call GetInformer.
	time.Sleep(50 * time.Millisecond)

	// source.Kind.Start calls GetInformer internally - should only happen once
	// due to the watching flag guard.
	if c := fc.GetInformerCallCount(); c != 1 {
		t.Errorf("expected GetInformer called once, got %d", c)
	}
}

func TestEnsure_CRDRemovedMidSync_FullRecovery(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Step 1: Ensure starts the watch, informer not synced -> false.
	if w.Ensure(t.Context()) {
		t.Fatal("expected false while informer not synced")
	}

	// Give the goroutine time to call GetInformer.
	time.Sleep(50 * time.Millisecond)

	// Step 2: CRD is removed before informer syncs.
	removedCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}
	dynamicwatch.SimulateCRDChange(w, t.Context(), removedCRD)

	// Step 3: After removal, Ensure should return false.
	if w.Ensure(t.Context()) {
		t.Fatal("expected false after CRD removal")
	}

	// Step 4: CRD is reinstalled.
	establishedCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
			},
		},
	}
	dynamicwatch.SimulateCRDChange(w, t.Context(), establishedCRD)

	// Step 5: Ensure should re-register (returns false, sync waiter pending).
	if w.Ensure(t.Context()) {
		t.Error("expected false after reinstall (sync waiter pending)")
	}

	// Give the goroutine time to call GetInformer again.
	time.Sleep(50 * time.Millisecond)

	// GetInformer should have been called twice (initial + re-registration).
	if c := fc.GetInformerCallCount(); c != 2 {
		t.Errorf("expected GetInformer called 2 times, got %d", c)
	}
}

func TestSimulateCRDChange_NotEstablished_DoesNotReRegister(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Get to ready state: start the watch, then wait for sync to complete.
	w.Ensure(t.Context())

	// Give the sync waiter time to call GetInformer and promote to active.
	time.Sleep(50 * time.Millisecond)

	// Verify the watch is active after sync.
	if !w.Available() {
		t.Fatal("expected available after sync")
	}

	// Simulate CRD status update with Established=False (mid-deletion, no
	// DeletionTimestamp yet). This is the critical pitfall scenario.
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionFalse},
			},
		},
	}
	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	// After the Established=False event, the watcher should be deactivated.
	if w.Available() {
		t.Error("expected watch to be deactivated")
	}

	// Ensure should return false - NOT attempt to re-register.
	if w.Ensure(t.Context()) {
		t.Error("expected false after Established=False event")
	}

	// GetInformer should have been called exactly once (initial registration
	// only - the Established=False event must NOT trigger re-registration).
	if c := fc.GetInformerCallCount(); c != 1 {
		t.Errorf("expected GetInformer called once (no re-registration), got %d", c)
	}
}

func TestConcurrent_OnCRDChange_And_Ensure(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		return []reconcile.Request{{}}
	})

	// Get to ready state: start the watch, then simulate sync completion.
	w.Ensure(t.Context())
	dynamicwatch.SetActive(w, true)

	removedCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	// Run onCRDChange and Ensure concurrently many times to stress the mutex.
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			dynamicwatch.SimulateCRDChange(w, t.Context(), removedCRD)
		})
		wg.Go(func() {
			w.Ensure(t.Context())
		})
	}
	wg.Wait()

	// After all CRD removal events, the watcher must be deactivated.
	if w.Available() {
		t.Error("expected watch to be deactivated after concurrent CRD removals")
	}
}

func TestConcurrent_OnCRDChange_And_TryGet(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetQueue(w, workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	))
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		return []reconcile.Request{{}}
	})
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)

	removedCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	// Run onCRDChange and TryGet concurrently to stress the mutex.
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			dynamicwatch.SimulateCRDChange(w, t.Context(), removedCRD)
		})
		wg.Go(func() {
			_, _ = w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})
		})
	}
	wg.Wait()

	// After all CRD removal events, the watcher must be deactivated.
	if w.Available() {
		t.Error("expected watch to be deactivated after concurrent CRD removals")
	}
}

func TestCRDAppearance_SetsCRDExists_EnsureRegisters(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Before CRD appears, Ensure should return false.
	if w.Ensure(t.Context()) {
		t.Fatal("expected false before CRD event")
	}

	// Simulate CRD established event.
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
			},
		},
	}
	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	// After CRD event, Ensure starts the watch but returns false while
	// the sync waiter goroutine hasn't promoted yet.
	if w.Ensure(t.Context()) {
		t.Error("expected false on first Ensure after CRD event (sync waiter pending)")
	}

	// Simulate the sync waiter completing by setting active directly.
	dynamicwatch.SetActive(w, true)

	if !w.Ensure(t.Context()) {
		t.Error("expected true after sync waiter promoted")
	}
}

func TestOnCRDChange_IncrementsGeneration(t *testing.T) {
	fc := &fakeCache{}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	genBefore := dynamicwatch.Generation(w)

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}
	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	genAfter := dynamicwatch.Generation(w)
	if genAfter != genBefore+1 {
		t.Errorf("expected generation to increment from %d to %d, got %d", genBefore, genBefore+1, genAfter)
	}
}

func TestCRDRemoval_ClearsCRDExists_EnsureReturnsFalse(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	// Simulate CRD removal.
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })
	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	// After removal, Ensure should return false.
	if w.Ensure(t.Context()) {
		t.Error("expected false after CRD removal")
	}
}

func TestWaitForSyncAndRequeue_Success_PromotesAndRequeues(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)

	var requeued atomic.Bool
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		requeued.Store(true)

		return []reconcile.Request{{}}
	})
	dynamicwatch.SetWatching(w, true)

	// Create a fake syncing source that succeeds immediately.
	src := &fakeSyncingSource{syncErr: nil}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 5*time.Second)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	if !w.Available() {
		t.Error("expected available after successful sync")
	}

	if !requeued.Load() {
		t.Error("expected requeueAll to be called after promotion")
	}
}

func TestWaitForSyncAndRequeue_StaleGeneration_DoesNotPromote(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetWatching(w, true)

	capturedGen := dynamicwatch.Generation(w)

	// Advance the generation to simulate CRD removal.
	dynamicwatch.SetGeneration(w, capturedGen+1)

	src := &fakeSyncingSource{syncErr: nil}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 5*time.Second)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, capturedGen, src)

	if w.Available() {
		t.Error("expected stale sync waiter to bail without promoting")
	}
}

func TestWaitForSyncAndRequeue_Timeout_ResetsWatchingAndRemovesInformer(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })
	dynamicwatch.SetWatching(w, true)

	// Source that returns an error (simulating timeout).
	src := &fakeSyncingSource{syncErr: context.DeadlineExceeded}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	if dynamicwatch.Watching(w) {
		t.Error("expected watching=false after sync timeout")
	}

	if w.Available() {
		t.Error("expected unavailable after sync timeout")
	}

	if fc.RemoveCallCount() != 1 {
		t.Errorf("expected 1 RemoveInformer call after timeout, got %d", fc.RemoveCallCount())
	}
}

func TestBuilder_WithSyncTimeout_PanicsOnZero(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when WithSyncTimeout called with zero duration")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "WithSyncTimeout") {
			t.Fatalf("panic should mention WithSyncTimeout, got: %v", r)
		}
	}()

	b := &dynamicwatch.WatcherBuilder[*corev1.ConfigMap]{}
	b.WithSyncTimeout(0)
}

func TestBuilder_WithSyncTimeout_PanicsOnNegative(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when WithSyncTimeout called with negative duration")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "WithSyncTimeout") {
			t.Fatalf("panic should mention WithSyncTimeout, got: %v", r)
		}
	}()

	b := &dynamicwatch.WatcherBuilder[*corev1.ConfigMap]{}
	b.WithSyncTimeout(-5 * time.Second)
}

func TestWatcher_DefaultSyncTimeout_Is30Seconds(t *testing.T) {
	w := newTestWatcher(&fakeCache{})

	timeout := dynamicwatch.SyncTimeout(w)
	expected := 30 * time.Second

	if timeout != expected {
		t.Errorf("expected default sync timeout to be %v, got %v", expected, timeout)
	}
}

func TestStatus_NotStarted(t *testing.T) {
	w := dynamicwatch.NewUnstartedTestWatcher[*corev1.ConfigMap](testCRDName, &fakeCache{})

	status := w.Status()

	if status.Available {
		t.Error("expected Available=false for NotStarted state")
	}

	if status.Reason != dynamicwatch.ReasonNotStarted {
		t.Errorf("expected Reason=NotStarted, got %s", status.Reason)
	}
}

func TestStatus_Ready(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetActive(w, true)

	status := w.Status()

	if !status.Available {
		t.Error("expected Available=true for Ready state")
	}

	if status.Reason != dynamicwatch.ReasonReady {
		t.Errorf("expected Reason=Ready, got %s", status.Reason)
	}
}

func TestStatus_CRDNotFound(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	// crdExists defaults to false, watching defaults to false

	status := w.Status()

	if status.Available {
		t.Error("expected Available=false for CRDNotFound state")
	}

	if status.Reason != dynamicwatch.ReasonCRDNotFound {
		t.Errorf("expected Reason=CRDNotFound, got %s", status.Reason)
	}
}

func TestStatus_Syncing(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetWatching(w, true)
	// active defaults to false

	status := w.Status()

	if status.Available {
		t.Error("expected Available=false for Syncing state")
	}

	if status.Reason != dynamicwatch.ReasonSyncing {
		t.Errorf("expected Reason=Syncing, got %s", status.Reason)
	}
}

func TestStatus_Pending(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetCRDExists(w, true)
	// watching defaults to false, active defaults to false

	status := w.Status()

	if status.Available {
		t.Error("expected Available=false for Pending state")
	}

	if status.Reason != dynamicwatch.ReasonPending {
		t.Errorf("expected Reason=Pending, got %s", status.Reason)
	}
}

// --- Metrics tests ---

func TestMetrics_Activated_GaugeAndCounter(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	gaugeBefore := testutil.ToFloat64(dynamicwatch.WatcherActive.WithLabelValues(testCRDName))
	counterBefore := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "activated"))

	src := &fakeSyncingSource{syncErr: nil}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 5*time.Second)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	gaugeAfter := testutil.ToFloat64(dynamicwatch.WatcherActive.WithLabelValues(testCRDName))
	counterAfter := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "activated"))

	if gaugeAfter != 1 {
		t.Errorf("expected gauge=1 after activation, got %v", gaugeAfter)
	}

	if counterAfter != counterBefore+1 {
		t.Errorf("expected activated counter to increment by 1, got delta %v (before=%v, after=%v)",
			counterAfter-counterBefore, counterBefore, counterAfter)
	}

	_ = gaugeBefore // gauge value before is not necessarily 0 due to other tests
}

func TestMetrics_Deactivated_GaugeAndCounter(t *testing.T) {
	fc := &fakeCache{}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		return []reconcile.Request{{}}
	})

	counterBefore := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "deactivated"))

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	gaugeAfter := testutil.ToFloat64(dynamicwatch.WatcherActive.WithLabelValues(testCRDName))
	counterAfter := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "deactivated"))

	if gaugeAfter != 0 {
		t.Errorf("expected gauge=0 after deactivation, got %v", gaugeAfter)
	}

	if counterAfter != counterBefore+1 {
		t.Errorf("expected deactivated counter to increment by 1, got delta %v (before=%v, after=%v)",
			counterAfter-counterBefore, counterBefore, counterAfter)
	}
}

func TestMetrics_Deactivated_NotRecordedWhenNotReady(t *testing.T) {
	fc := &fakeCache{}
	w := newStartedWatcher(fc)
	// Watch is registered (watching=true) but NOT active (active=false).
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	counterBefore := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "deactivated"))

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	counterAfter := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "deactivated"))

	if counterAfter != counterBefore {
		t.Errorf("expected deactivated counter NOT to increment when wasReady=false, got delta %v",
			counterAfter-counterBefore)
	}
}

func TestMetrics_Invalidated_GaugeAndCounter(t *testing.T) {
	fc := &fakeCache{getErr: &cache.ErrResourceNotCached{}}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)

	counterBefore := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "invalidated"))

	_, _ = w.TryGet(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	gaugeAfter := testutil.ToFloat64(dynamicwatch.WatcherActive.WithLabelValues(testCRDName))
	counterAfter := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "invalidated"))

	if gaugeAfter != 0 {
		t.Errorf("expected gauge=0 after invalidation, got %v", gaugeAfter)
	}

	if counterAfter != counterBefore+1 {
		t.Errorf("expected invalidated counter to increment by 1, got delta %v (before=%v, after=%v)",
			counterAfter-counterBefore, counterBefore, counterAfter)
	}
}

func TestMetrics_SyncFailed_Counter(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	counterBefore := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "sync_failed"))

	src := &fakeSyncingSource{syncErr: context.DeadlineExceeded}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	counterAfter := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "sync_failed"))

	if counterAfter != counterBefore+1 {
		t.Errorf("expected sync_failed counter to increment by 1, got delta %v (before=%v, after=%v)",
			counterAfter-counterBefore, counterBefore, counterAfter)
	}

	// Gauge should remain at whatever it was (not set to 1).
	gaugeAfter := testutil.ToFloat64(dynamicwatch.WatcherActive.WithLabelValues(testCRDName))
	if gaugeAfter != 0 {
		t.Errorf("expected gauge=0 after sync failure, got %v", gaugeAfter)
	}
}

func TestMetrics_SyncFailed_StaleGeneration_NoCounter(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetWatching(w, true)

	capturedGen := dynamicwatch.Generation(w)

	// Advance generation to simulate CRD removal.
	dynamicwatch.SetGeneration(w, capturedGen+1)

	counterBefore := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "sync_failed"))

	src := &fakeSyncingSource{syncErr: context.DeadlineExceeded}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, capturedGen, src)

	counterAfter := testutil.ToFloat64(dynamicwatch.WatcherTransitions.WithLabelValues(testCRDName, "sync_failed"))

	if counterAfter != counterBefore {
		t.Errorf("expected sync_failed counter NOT to increment for stale generation, got delta %v",
			counterAfter-counterBefore)
	}
}

// --- Event recorder tests ---

func TestEvent_Activation(t *testing.T) {
	fc := &fakeCache{}
	recorder := record.NewFakeRecorder(10)
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetRecorder(w, recorder)
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	src := &fakeSyncingSource{syncErr: nil}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 5*time.Second)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "WatchActivated") {
			t.Errorf("expected WatchActivated event, got: %s", event)
		}
	case <-time.After(time.Second):
		t.Fatal("expected event but none received")
	}
}

func TestEvent_Deactivation(t *testing.T) {
	fc := &fakeCache{}
	recorder := record.NewFakeRecorder(10)
	w := newStartedWatcher(fc)
	dynamicwatch.SetRecorder(w, recorder)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		return []reconcile.Request{{}}
	})

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	}

	dynamicwatch.SimulateCRDChange(w, t.Context(), crd)

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "WatchDeactivated") {
			t.Errorf("expected WatchDeactivated event, got: %s", event)
		}
	case <-time.After(time.Second):
		t.Fatal("expected event but none received")
	}
}

func TestEvent_SyncFailure(t *testing.T) {
	fc := &fakeCache{}
	recorder := record.NewFakeRecorder(10)
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	dynamicwatch.SetRecorder(w, recorder)
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	src := &fakeSyncingSource{syncErr: context.DeadlineExceeded}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "WatchSyncFailed") {
			t.Errorf("expected WatchSyncFailed event, got: %s", event)
		}
	case <-time.After(time.Second):
		t.Fatal("expected event but none received")
	}
}

func TestEvent_NilRecorder_NoPanic(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	q := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	dynamicwatch.SetQueue(w, q)
	// No recorder set - default is nil.
	dynamicwatch.SetWatching(w, true)
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Should not panic with nil recorder.
	src := &fakeSyncingSource{syncErr: nil}
	syncCtx, syncCancel := context.WithTimeout(t.Context(), 5*time.Second)

	dynamicwatch.CallWaitForSyncAndRequeue(w, syncCtx, syncCancel, dynamicwatch.Generation(w), src)

	if !w.Available() {
		t.Error("expected available after successful sync even without recorder")
	}
}

func TestBuilder_WithEventRecorder_PanicsOnNil(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when WithEventRecorder called with nil recorder")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "WithEventRecorder") {
			t.Fatalf("panic should mention WithEventRecorder, got: %v", r)
		}
	}()

	b := &dynamicwatch.WatcherBuilder[*corev1.ConfigMap]{}
	b.WithEventRecorder(nil)
}
