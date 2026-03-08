package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"
)

const (
	pluginConfigCRDName = "pluginconfigs.demo.example.com"
	pluginRefField      = "spec.pluginRef"
)

// WidgetReconciler reconciles a Widget object.
type WidgetReconciler struct {
	client.Client

	Scheme      *runtime.Scheme
	recorder    events.EventRecorder
	pluginWatch *dynamicwatch.Watcher[*demov1alpha1.PluginConfig]
}

// +kubebuilder:rbac:groups=demo.example.com,resources=widgets,verbs=get;list;watch
// +kubebuilder:rbac:groups=demo.example.com,resources=widgets/status,verbs=get;patch
// +kubebuilder:rbac:groups=demo.example.com,resources=pluginconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	widget := &demov1alpha1.Widget{}
	if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	patch := client.MergeFrom(widget.DeepCopy())

	if widget.Spec.PluginRef == "" {
		if widget.HasPluginCondition() {
			widget.RemovePluginCondition()

			return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
		}

		return ctrl.Result{}, nil
	}

	switch r.pluginWatch.Ensure(ctx) {
	case dynamicwatch.JustRegistered:
		r.recorder.Eventf(widget, nil, corev1.EventTypeNormal, "PluginWatchRegistered",
			"RegisterWatch", "Dynamic watch for PluginConfig CRD activated")

		return ctrl.Result{RequeueAfter: time.Second}, nil
	case dynamicwatch.NotAvailable:
		widget.MarkPluginCRDNotAvailable()

		return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
	case dynamicwatch.Active:
		// Watch already running, proceed to read PluginConfig.
	}

	// CRD is available - try to read the referenced PluginConfig.
	plugin := &demov1alpha1.PluginConfig{}
	pluginKey := client.ObjectKey{Name: widget.Spec.PluginRef, Namespace: widget.Namespace}

	if err := r.pluginWatch.Get(ctx, pluginKey, plugin); err != nil {
		if errors.Is(err, dynamicwatch.ErrCacheInvalidated) {
			r.recorder.Eventf(widget, nil, corev1.EventTypeWarning, "PluginCacheInvalidated",
				"CacheInvalidated", "PluginConfig informer was removed during reconciliation; requeuing")

			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		if client.IgnoreNotFound(err) == nil {
			log.Info("Referenced PluginConfig not found", "pluginRef", widget.Spec.PluginRef)
			widget.MarkPluginNotFound()

			return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
		}

		return ctrl.Result{}, err
	}

	log.Info("Applied PluginConfig", "pluginRef", widget.Spec.PluginRef, "setting", plugin.Spec.Setting)
	widget.MarkPluginApplied(plugin.Spec.Setting)

	return ctrl.Result{}, r.Status().Patch(ctx, widget, patch)
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
	r.recorder = mgr.GetEventRecorder("widget-controller")

	var err error
	r.pluginWatch, err = dynamicwatch.For[*demov1alpha1.PluginConfig](mgr, pluginConfigCRDName).
		EnqueueOnObjectChange(r.pluginConfigToWidgets).
		EnqueueOnCRDChange(r.allWidgetsWithPluginRef).
		Build()
	if err != nil {
		return fmt.Errorf("setting up plugin watch: %w", err)
	}

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

	b := ctrl.NewControllerManagedBy(mgr).
		For(&demov1alpha1.Widget{}).
		Named("widget")

	r.pluginWatch.Register(b)

	c, err := b.Build(r)
	if err != nil {
		return err
	}

	r.pluginWatch.Bind(c)

	return nil
}
