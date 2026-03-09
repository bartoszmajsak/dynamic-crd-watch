package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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

	themeCRDName  = "themes.demo.example.com"
	themeRefField = "spec.themeRef"

	hasPluginRefField = "has-pluginRef"
	hasThemeRefField  = "has-themeRef"
)

// WidgetReconciler reconciles a Widget object.
type WidgetReconciler struct {
	client.Client

	Scheme      *runtime.Scheme
	recorder    events.EventRecorder
	pluginWatch *dynamicwatch.Watcher[*demov1alpha1.PluginConfig]
	themeWatch  *dynamicwatch.Watcher[*demov1alpha1.Theme]
}

// +kubebuilder:rbac:groups=demo.example.com,resources=widgets,verbs=get;list;watch
// +kubebuilder:rbac:groups=demo.example.com,resources=widgets/status,verbs=get;patch
// +kubebuilder:rbac:groups=demo.example.com,resources=pluginconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=demo.example.com,resources=themes,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	widget := &demov1alpha1.Widget{}
	if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	oldStatus := widget.Status.DeepCopy()
	patch := client.MergeFrom(widget.DeepCopy())
	widget.Status.ObservedGeneration = widget.Generation

	pluginResult, pluginErr := r.reconcilePlugin(ctx, widget)
	themeResult, themeErr := r.reconcileTheme(ctx, widget)

	if err := errors.Join(pluginErr, themeErr); err != nil {
		return ctrl.Result{}, err
	}

	if !equality.Semantic.DeepEqual(oldStatus, &widget.Status) {
		if err := r.Status().Patch(ctx, widget, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	return mergeResults(pluginResult, themeResult), nil
}

//nolint:dupl // Structural similarity with reconcileTheme is intentional; each handles a different type.
func (r *WidgetReconciler) reconcilePlugin(ctx context.Context, widget *demov1alpha1.Widget) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if widget.Spec.PluginRef == "" {
		widget.RemovePluginCondition()

		return ctrl.Result{}, nil
	}

	switch r.pluginWatch.Ensure(ctx) {
	case dynamicwatch.JustRegistered:
		r.recorder.Eventf(widget, nil, corev1.EventTypeNormal, "PluginWatchRegistered",
			"RegisterWatch", "Dynamic watch for PluginConfig CRD activated")

		return ctrl.Result{RequeueAfter: time.Second}, nil
	case dynamicwatch.NotAvailable:
		widget.MarkPluginCRDNotAvailable()

		return ctrl.Result{}, nil
	case dynamicwatch.Active:
		// Watch already running, proceed to read PluginConfig.
	}

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
			widget.MarkPluginNotFound(widget.Spec.PluginRef)

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	log.Info("Applied PluginConfig", "pluginRef", widget.Spec.PluginRef, "setting", plugin.Spec.Setting)
	widget.MarkPluginApplied(plugin.Spec.Setting)

	return ctrl.Result{}, nil
}

//nolint:dupl // Structural similarity with reconcilePlugin is intentional; each handles a different type.
func (r *WidgetReconciler) reconcileTheme(ctx context.Context, widget *demov1alpha1.Widget) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if widget.Spec.ThemeRef == "" {
		widget.RemoveThemeCondition()

		return ctrl.Result{}, nil
	}

	switch r.themeWatch.Ensure(ctx) {
	case dynamicwatch.JustRegistered:
		r.recorder.Eventf(widget, nil, corev1.EventTypeNormal, "ThemeWatchRegistered",
			"RegisterWatch", "Dynamic watch for Theme CRD activated")

		return ctrl.Result{RequeueAfter: time.Second}, nil
	case dynamicwatch.NotAvailable:
		widget.MarkThemeCRDNotAvailable()

		return ctrl.Result{}, nil
	case dynamicwatch.Active:
		// Watch already running, proceed to read Theme.
	}

	theme := &demov1alpha1.Theme{}
	themeKey := client.ObjectKey{Name: widget.Spec.ThemeRef, Namespace: widget.Namespace}

	if err := r.themeWatch.Get(ctx, themeKey, theme); err != nil {
		if errors.Is(err, dynamicwatch.ErrCacheInvalidated) {
			r.recorder.Eventf(widget, nil, corev1.EventTypeWarning, "ThemeCacheInvalidated",
				"CacheInvalidated", "Theme informer was removed during reconciliation; requeuing")

			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		if client.IgnoreNotFound(err) == nil {
			log.Info("Referenced Theme not found", "themeRef", widget.Spec.ThemeRef)
			widget.MarkThemeNotFound(widget.Spec.ThemeRef)

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	log.Info("Applied Theme", "themeRef", widget.Spec.ThemeRef, "colorScheme", theme.Spec.ColorScheme)
	widget.MarkThemeApplied(theme.Spec.ColorScheme)

	return ctrl.Result{}, nil
}

// mergeResults returns the result with the shortest RequeueAfter, preferring
// requeue over no-requeue.
func mergeResults(results ...ctrl.Result) ctrl.Result {
	var merged ctrl.Result
	for _, r := range results {
		if r.RequeueAfter > 0 && (merged.RequeueAfter == 0 || r.RequeueAfter < merged.RequeueAfter) {
			merged.RequeueAfter = r.RequeueAfter
		}
	}

	return merged
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
	if err := r.List(ctx, &widgets, client.MatchingFields{hasPluginRefField: "true"}); err != nil {
		log.Error(err, "Failed to list Widgets for requeue")

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

// themeToWidgets maps a Theme event to reconcile requests for
// all Widgets that reference it by name via the field index.
func (r *WidgetReconciler) themeToWidgets(ctx context.Context, obj *demov1alpha1.Theme) []reconcile.Request {
	log := logf.FromContext(ctx)

	var widgets demov1alpha1.WidgetList
	if err := r.List(ctx, &widgets, client.MatchingFields{themeRefField: obj.GetName()}); err != nil {
		log.Error(err, "Failed to list Widgets for Theme mapping")

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

// allWidgetsWithThemeRef returns reconcile requests for all Widgets that have
// a themeRef set, so they can re-evaluate their ThemeReady condition.
func (r *WidgetReconciler) allWidgetsWithThemeRef(ctx context.Context) []reconcile.Request {
	log := logf.FromContext(ctx)

	var widgets demov1alpha1.WidgetList
	if err := r.List(ctx, &widgets, client.MatchingFields{hasThemeRefField: "true"}); err != nil {
		log.Error(err, "Failed to list Widgets for requeue")

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

// SetupWithManager sets up the controller with the Manager.
func (r *WidgetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.recorder = mgr.GetEventRecorder("widget-controller")

	crdCache, err := dynamicwatch.NewCRDCache(mgr)
	if err != nil {
		return fmt.Errorf("creating shared CRD cache: %w", err)
	}

	r.pluginWatch, err = dynamicwatch.For[*demov1alpha1.PluginConfig](mgr, pluginConfigCRDName).
		WithCRDCache(crdCache).
		EnqueueOnObjectChange(r.pluginConfigToWidgets).
		EnqueueOnCRDChange(r.allWidgetsWithPluginRef).
		Build()
	if err != nil {
		return fmt.Errorf("setting up plugin watch: %w", err)
	}

	r.themeWatch, err = dynamicwatch.For[*demov1alpha1.Theme](mgr, themeCRDName).
		WithCRDCache(crdCache).
		EnqueueOnObjectChange(r.themeToWidgets).
		EnqueueOnCRDChange(r.allWidgetsWithThemeRef).
		Build()
	if err != nil {
		return fmt.Errorf("setting up theme watch: %w", err)
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

	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&demov1alpha1.Widget{},
		themeRefField,
		func(obj client.Object) []string {
			w, ok := obj.(*demov1alpha1.Widget)
			if !ok || w.Spec.ThemeRef == "" {
				return nil
			}

			return []string{w.Spec.ThemeRef}
		},
	); err != nil {
		return fmt.Errorf("indexing %s: %w", themeRefField, err)
	}

	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&demov1alpha1.Widget{},
		hasPluginRefField,
		func(obj client.Object) []string {
			w, ok := obj.(*demov1alpha1.Widget)
			if !ok || w.Spec.PluginRef == "" {
				return nil
			}

			return []string{"true"}
		},
	); err != nil {
		return fmt.Errorf("indexing %s: %w", hasPluginRefField, err)
	}

	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&demov1alpha1.Widget{},
		hasThemeRefField,
		func(obj client.Object) []string {
			w, ok := obj.(*demov1alpha1.Widget)
			if !ok || w.Spec.ThemeRef == "" {
				return nil
			}

			return []string{"true"}
		},
	); err != nil {
		return fmt.Errorf("indexing %s: %w", hasThemeRefField, err)
	}

	b := ctrl.NewControllerManagedBy(mgr).
		For(&demov1alpha1.Widget{}).
		Named("widget")

	r.pluginWatch.Register(b)
	r.themeWatch.Register(b)

	c, err := b.Build(r)
	if err != nil {
		return err
	}

	r.pluginWatch.Bind(c)
	r.themeWatch.Bind(c)

	return nil
}
