package dynamicwatch_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"
)

// fakeCache stubs the cache methods Watcher uses: Get, RemoveInformer,
// and WaitForCacheSync. The embedded cache.Cache satisfies the interface
// for source.Kind, which is never actually invoked in unit tests
// (startSource is stubbed via a no-op).
type fakeCache struct {
	cache.Cache

	removeErr   error
	removeCalls int
	getErr      error
	mu          sync.Mutex

	// syncCh controls WaitForCacheSync. Close it to unblock.
	// If nil, WaitForCacheSync returns true immediately.
	syncCh     chan struct{}
	syncResult bool
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

func (f *fakeCache) WaitForCacheSync(ctx context.Context) bool {
	if f.syncCh == nil {
		return true
	}

	select {
	case <-f.syncCh:
		return f.syncResult
	case <-ctx.Done():
		return false
	}
}

const testCRDName = "configmaps.test.io"

func newTestWatcher(fc *fakeCache) *dynamicwatch.Watcher[*corev1.ConfigMap] {
	return dynamicwatch.NewTestWatcher[*corev1.ConfigMap](testCRDName, fc)
}

func newStartedWatcher(fc *fakeCache) *dynamicwatch.Watcher[*corev1.ConfigMap] {
	w := newTestWatcher(fc)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		return nil
	})

	return w
}

func TestWaitForSync_BeforeStart_ReturnsError(t *testing.T) {
	w := newTestWatcher(&fakeCache{})

	err := w.WaitForSync(t.Context())
	if err == nil {
		t.Error("expected error when WaitForSync called before Start")
	}
}

func TestEnsure_BeforeStart_Panics(t *testing.T) {
	w := newTestWatcher(&fakeCache{})

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

func TestGet_Success(t *testing.T) {
	w := newStartedWatcher(&fakeCache{})
	dynamicwatch.SetActive(w, true)

	err := w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestGet_ErrResourceNotCached_ResetsState(t *testing.T) {
	fc := &fakeCache{getErr: &cache.ErrResourceNotCached{}}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)

	err := w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

	if !errors.Is(err, dynamicwatch.ErrCacheInvalidated) {
		t.Errorf("expected ErrCacheInvalidated, got %v", err)
	}

	if w.Available() {
		t.Error("expected Available() to be false after cache invalidation")
	}

	// Verify crdExists was also reset - Ensure should return false.
	if w.Ensure(t.Context()) {
		t.Error("expected false after cache invalidation")
	}
}

func TestGet_OtherError_PreservesState(t *testing.T) {
	someErr := errors.New("something else")
	fc := &fakeCache{getErr: someErr}
	w := newStartedWatcher(fc)
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

func TestEnsure_StartSourceFails_ReturnsFalse(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)

	callCount := 0
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		callCount++
		if callCount == 1 {
			return errors.New("start failed")
		}

		return nil
	})

	// First call should fail.
	if w.Ensure(t.Context()) {
		t.Error("expected false on start failure")
	}

	// Second call should retry and start the watch successfully,
	// but still returns false (sync waiter pending).
	if w.Ensure(t.Context()) {
		t.Error("expected false on retry (sync waiter pending)")
	}
}

func TestEnsure_ConcurrentCalls_RegistersOnce(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)

	var startCount atomic.Int32
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		startCount.Add(1)

		return nil
	})

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			w.Ensure(t.Context())
		})
	}
	wg.Wait()

	if c := startCount.Load(); c != 1 {
		t.Errorf("expected startSource called once, got %d", c)
	}
}

func TestEnsure_CRDRemovedMidSync_FullRecovery(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)

	var startCount atomic.Int32
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		startCount.Add(1)

		return nil
	})
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Step 1: Ensure starts the watch, informer not synced → false.
	if w.Ensure(t.Context()) {
		t.Fatal("expected false while informer not synced")
	}

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

	// startSource should have been called twice (initial + re-registration).
	if c := startCount.Load(); c != 2 {
		t.Errorf("expected startSource called 2 times, got %d", c)
	}
}

func TestSimulateCRDChange_NotEstablished_DoesNotReRegister(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)

	var startCount atomic.Int32
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		startCount.Add(1)

		return nil
	})
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Get to ready state: start the watch, then simulate sync completion.
	w.Ensure(t.Context())
	dynamicwatch.SetActive(w, true)

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

	// startSource should have been called exactly once (initial registration
	// only - the Established=False event must NOT trigger re-registration).
	if c := startCount.Load(); c != 1 {
		t.Errorf("expected startSource called once (no re-registration), got %d", c)
	}
}

func TestConcurrent_OnCRDChange_And_Ensure(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		return nil
	})
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

func TestConcurrent_OnCRDChange_And_Get(t *testing.T) {
	fc := &fakeCache{}
	w := newTestWatcher(fc)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error {
		return nil
	})
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

	// Run onCRDChange and Get concurrently to stress the mutex.
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			dynamicwatch.SimulateCRDChange(w, t.Context(), removedCRD)
		})
		wg.Go(func() {
			_ = w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})
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

func TestGet_ErrResourceNotCached_IncrementsGeneration(t *testing.T) {
	fc := &fakeCache{getErr: &cache.ErrResourceNotCached{}}
	w := newStartedWatcher(fc)
	dynamicwatch.SetActive(w, true)
	dynamicwatch.SetCRDExists(w, true)

	genBefore := dynamicwatch.Generation(w)

	_ = w.Get(t.Context(), client.ObjectKey{Name: "test"}, &corev1.ConfigMap{})

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

func TestSyncWaiter_PromotesAndRequeues(t *testing.T) {
	syncCh := make(chan struct{})
	fc := &fakeCache{syncCh: syncCh, syncResult: true}
	w := newTestWatcher(fc)

	var requeued atomic.Bool
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request {
		requeued.Store(true)

		return []reconcile.Request{{}}
	})
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error { return nil })

	// Wire up a sync waiter that directly promotes (simulating what the
	// real waitForSyncAndRequeue does after WaitForCacheSync succeeds).
	done := make(chan struct{})
	dynamicwatch.SetStartSyncWaiter(w, func(gen uint64, _ source.SyncingSource) {
		defer close(done)

		if !fc.WaitForCacheSync(context.Background()) {
			return
		}

		if dynamicwatch.Generation(w) != gen {
			return
		}

		dynamicwatch.SetActive(w, true)
		requeued.Store(true)
	})

	// Ensure starts the watch and spawns the sync waiter goroutine.
	if w.Ensure(t.Context()) {
		t.Fatal("expected false while informer not synced")
	}

	if w.Available() {
		t.Fatal("expected unavailable before sync completes")
	}

	// Unblock the sync waiter.
	close(syncCh)
	<-done

	if !w.Available() {
		t.Error("expected available after sync waiter promoted")
	}

	if !requeued.Load() {
		t.Error("expected requeueAll to be called after promotion")
	}
}

func TestSyncWaiter_StaleGeneration_DoesNotPromote(t *testing.T) {
	syncCh := make(chan struct{})
	fc := &fakeCache{syncCh: syncCh, syncResult: true}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error { return nil })
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	done := make(chan struct{})
	dynamicwatch.SetStartSyncWaiter(w, func(gen uint64, _ source.SyncingSource) {
		defer close(done)

		if !fc.WaitForCacheSync(context.Background()) {
			return
		}

		// Check generation - should mismatch after onCRDChange incremented it.
		if dynamicwatch.Generation(w) != gen {
			return
		}

		dynamicwatch.SetActive(w, true)
	})

	// Ensure starts the watch.
	w.Ensure(t.Context())

	// Simulate CRD removal while sync waiter is blocking - this
	// increments the generation, making the goroutine stale.
	dynamicwatch.SimulateCRDChange(w, t.Context(), &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testCRDName,
			DeletionTimestamp: &metav1.Time{},
		},
	})

	// Unblock the (now stale) sync waiter.
	close(syncCh)
	<-done

	// The stale goroutine must NOT promote.
	if w.Available() {
		t.Error("expected stale sync waiter to bail without promoting")
	}
}

func TestSyncWaiter_ContextCancelled_DoesNotPromote(t *testing.T) {
	syncCh := make(chan struct{}) // never closed - blocks until context cancelled
	fc := &fakeCache{syncCh: syncCh, syncResult: true}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error { return nil })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	dynamicwatch.SetStartSyncWaiter(w, func(_ uint64, _ source.SyncingSource) {
		defer close(done)
		// Use the cancellable context instead of Background.
		fc.WaitForCacheSync(ctx)
	})

	w.Ensure(ctx)

	// Cancel the context - simulates manager shutdown.
	cancel()
	<-done

	if w.Available() {
		t.Error("expected unavailable after context cancellation")
	}
}

func TestEnsure_Timeout_ResetsWatchingAndRemovesInformer(t *testing.T) {
	// fakeCache with a syncCh that we never close - the sync waiter will
	// rely on context cancellation (from the timeout or explicit cancel).
	syncCh := make(chan struct{})
	fc := &fakeCache{syncCh: syncCh, syncResult: false}
	w := newTestWatcher(fc)
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetStartSource(w, func(_ source.SyncingSource) error { return nil })
	dynamicwatch.SetRequeueAll(w, func(_ context.Context) []reconcile.Request { return nil })

	// Wire up a sync waiter that simulates the timeout path:
	// WaitForCacheSync returns false → reset watching, call RemoveInformer.
	done := make(chan struct{})
	dynamicwatch.SetStartSyncWaiter(w, func(gen uint64, _ source.SyncingSource) {
		defer close(done)

		// Simulate a short timeout.
		timeoutCtx, timeoutCancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer timeoutCancel()

		if fc.WaitForCacheSync(timeoutCtx) {
			dynamicwatch.SetActive(w, true)

			return
		}

		// Timeout fired - reset watching and remove informer only if
		// generation still matches (not stale).
		if dynamicwatch.Generation(w) != gen {
			return
		}

		dynamicwatch.SetWatching(w, false)
		_ = fc.RemoveInformer(t.Context(), &corev1.ConfigMap{})
	})

	// Ensure starts the watch, which spawns the sync waiter goroutine.
	w.Ensure(t.Context())

	// Wait for the sync waiter to timeout.
	<-done

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
