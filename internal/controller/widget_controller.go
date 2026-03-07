package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
)

const (
	ConditionPluginReady = "PluginReady"

	ReasonPluginCRDNotAvailable = "PluginCRDNotAvailable"
	ReasonPluginApplied         = "PluginApplied"
	ReasonPluginNotFound        = "PluginNotFound"

	pluginConfigCRDName = "pluginconfigs.demo.example.com"
	pluginRefField      = "spec.pluginRef"
)

// WidgetReconciler reconciles a Widget object.
type WidgetReconciler struct {
	client.Client

	Scheme *runtime.Scheme

	// Fields for dynamic watch management.
	ctrl              controller.Controller
	cache             cache.Cache
	discoveryClient   *discovery.DiscoveryClient
	mu                sync.Mutex
	pluginWatchActive bool
}

// +kubebuilder:rbac:groups=demo.example.com,resources=widgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=demo.example.com,resources=widgets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=demo.example.com,resources=widgets/finalizers,verbs=update
// +kubebuilder:rbac:groups=demo.example.com,resources=pluginconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	widget := &demov1alpha1.Widget{}
	if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	patch := client.MergeFrom(widget.DeepCopy())

	if widget.Spec.PluginRef == "" {
		if meta.FindStatusCondition(widget.Status.Conditions, ConditionPluginReady) != nil {
			meta.RemoveStatusCondition(&widget.Status.Conditions, ConditionPluginReady)

			return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
		}

		return ctrl.Result{}, nil
	}

	if !r.ensurePluginWatch(ctx) {
		meta.SetStatusCondition(&widget.Status.Conditions, metav1.Condition{
			Type:               ConditionPluginReady,
			Status:             metav1.ConditionFalse,
			Reason:             ReasonPluginCRDNotAvailable,
			Message:            "PluginConfig CRD is not installed",
			ObservedGeneration: widget.Generation,
		})

		return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
	}

	// CRD is available — try to read the referenced PluginConfig.
	plugin := &demov1alpha1.PluginConfig{}
	pluginKey := client.ObjectKey{Name: widget.Spec.PluginRef, Namespace: widget.Namespace}

	if err := r.Get(ctx, pluginKey, plugin); err != nil {
		// If the informer was removed (CRD disappeared between ensurePluginWatch and here),
		// reset the watch state and requeue so the next reconcile detects CRD absence properly.
		var notCached *cache.ErrResourceNotCached
		if errors.As(err, &notCached) {
			r.mu.Lock()
			r.pluginWatchActive = false
			r.mu.Unlock()

			return ctrl.Result{Requeue: true}, nil
		}

		if client.IgnoreNotFound(err) == nil {
			log.Info("Referenced PluginConfig not found", "pluginRef", widget.Spec.PluginRef)
			meta.SetStatusCondition(&widget.Status.Conditions, metav1.Condition{
				Type:               ConditionPluginReady,
				Status:             metav1.ConditionFalse,
				Reason:             ReasonPluginNotFound,
				Message:            "Referenced PluginConfig does not exist",
				ObservedGeneration: widget.Generation,
			})

			return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
		}

		return ctrl.Result{}, err
	}

	log.Info("Applied PluginConfig", "pluginRef", widget.Spec.PluginRef, "setting", plugin.Spec.Setting)
	meta.SetStatusCondition(&widget.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonPluginApplied,
		Message:            plugin.Spec.Setting,
		ObservedGeneration: widget.Generation,
	})

	return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
}

// ensurePluginWatch checks if the PluginConfig CRD is available and registers
// a dynamic watch if it is. Returns true if the watch is active.
//
// Uses a check-lock-check pattern to avoid holding the mutex during
// the network call to the discovery API.
func (r *WidgetReconciler) ensurePluginWatch(ctx context.Context) bool {
	r.mu.Lock()
	if r.pluginWatchActive {
		r.mu.Unlock()

		return true
	}
	r.mu.Unlock()

	if !r.isCRDAvailable(ctx) {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check: another goroutine may have registered while we were checking discovery.
	if r.pluginWatchActive {
		return true
	}

	log := logf.FromContext(ctx)

	// Register a watch for PluginConfig changes. When a PluginConfig is
	// created/updated/deleted, requeue all Widgets that reference it by name.
	src := source.Kind(r.cache, &demov1alpha1.PluginConfig{},
		handler.TypedEnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj *demov1alpha1.PluginConfig) []reconcile.Request {
				return r.pluginConfigToWidgets(ctx, obj)
			},
		),
	)

	if err := r.ctrl.Watch(src); err != nil {
		log.Error(err, "Failed to register PluginConfig watch")

		return false
	}

	r.pluginWatchActive = true
	log.Info("Dynamically registered watch for PluginConfig")

	return true
}

// isCRDAvailable checks the discovery API to determine if the PluginConfig
// CRD is installed. Uses a direct discovery call (not cached) for freshness.
func (r *WidgetReconciler) isCRDAvailable(ctx context.Context) bool {
	resources, err := r.discoveryClient.ServerResourcesForGroupVersion(demov1alpha1.GroupVersion.String())
	if err != nil {
		logf.FromContext(ctx).V(1).Info("PluginConfig group not available via discovery", "error", err)

		return false
	}

	for i := range resources.APIResources {
		if resources.APIResources[i].Kind == "PluginConfig" {
			return true
		}
	}

	return false
}

// onCRDChange handles CRD create/delete events for the PluginConfig CRD.
// On creation: requeue all Widgets with pluginRef so they can register the watch.
// On deletion: clean up the informer and requeue affected Widgets.
//
// Note: this handler runs on the controller work queue's event processing goroutine,
// not on a reconcile worker. The mutex protects against concurrent reconcile access
// to pluginWatchActive.
func (r *WidgetReconciler) onCRDChange(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)

	crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return nil
	}

	if crd.Name != pluginConfigCRDName {
		return nil
	}

	crdBeingRemoved := !crd.DeletionTimestamp.IsZero() || !isCRDEstablished(crd)
	if crdBeingRemoved {
		r.mu.Lock()
		if r.pluginWatchActive {
			if err := r.cache.RemoveInformer(ctx, &demov1alpha1.PluginConfig{}); err != nil {
				log.Error(err, "Failed to remove PluginConfig informer")
			}

			r.pluginWatchActive = false
			log.Info("Removed PluginConfig watch after CRD deletion")
		}
		r.mu.Unlock()
	} else {
		log.Info("PluginConfig CRD detected, will requeue affected Widgets")
	}

	return r.allWidgetsWithPluginRef(ctx)
}

// pluginConfigToWidgets maps a PluginConfig event to reconcile requests for
// all Widgets that reference it by name via the field index.
func (r *WidgetReconciler) pluginConfigToWidgets(ctx context.Context, obj *demov1alpha1.PluginConfig) []reconcile.Request {
	log := logf.FromContext(ctx)

	var widgets demov1alpha1.WidgetList
	if err := r.List(ctx, &widgets, client.MatchingFields{pluginRefField: obj.GetName()}); err != nil {
		log.Error(err, "Failed to list Widgets for PluginConfig mapping")

		return nil
	}

	requests := make([]reconcile.Request, 0, len(widgets.Items))
	for i := range widgets.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&widgets.Items[i]),
		})
	}

	return requests
}

// allWidgetsWithPluginRef returns reconcile requests for all Widgets that have
// a pluginRef set, so they can re-evaluate their PluginReady condition.
//
// Note: EnqueueRequestsFromMapFunc does not support returning errors.
// If the List fails, affected Widgets won't be requeued until the next
// periodic resync or another triggering event.
func (r *WidgetReconciler) allWidgetsWithPluginRef(ctx context.Context) []reconcile.Request {
	log := logf.FromContext(ctx)

	var widgets demov1alpha1.WidgetList
	if err := r.List(ctx, &widgets); err != nil {
		log.Error(err, "Failed to list Widgets for requeue")

		return nil
	}

	var requests []reconcile.Request
	for i := range widgets.Items {
		if widgets.Items[i].Spec.PluginRef != "" {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&widgets.Items[i]),
			})
		}
	}

	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *WidgetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.cache = mgr.GetCache()

	dc, err := discovery.NewDiscoveryClientForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("creating discovery client: %w", err)
	}
	r.discoveryClient = dc

	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&demov1alpha1.Widget{},
		pluginRefField,
		func(obj client.Object) []string {
			w, ok := obj.(*demov1alpha1.Widget)
			if !ok || w.Spec.PluginRef == "" {
				return nil
			}

			return []string{w.Spec.PluginRef}
		},
	); err != nil {
		return fmt.Errorf("indexing %s: %w", pluginRefField, err)
	}

	r.ctrl, err = ctrl.NewControllerManagedBy(mgr).
		For(&demov1alpha1.Widget{}).
		Named("widget").
		// Watch CRD resources, scoped to only the PluginConfig CRD by name.
		Watches(&apiextensionsv1.CustomResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.onCRDChange),
			builder.WithPredicates(crdNamePredicate(pluginConfigCRDName)),
		).
		Build(r)

	return err
}

// isCRDEstablished checks whether a CRD has the Established condition set to True.
func isCRDEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, c := range crd.Status.Conditions {
		if c.Type == apiextensionsv1.Established {
			return c.Status == apiextensionsv1.ConditionTrue
		}
	}

	return false
}

// crdNamePredicate filters CRD events to only those matching the given CRD name.
// This ensures the informer delivers events but we only process the one we care about,
// avoiding unnecessary reconcile triggers for unrelated CRDs.
func crdNamePredicate(name string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetName() == name
	})
}
