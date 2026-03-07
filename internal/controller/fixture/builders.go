package fixture

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
)

// WidgetOption is a functional option for building Widget test resources.
type WidgetOption func(*demov1alpha1.Widget)

// Widget creates a Widget resource with the given name and options.
func Widget(name, namespace string, opts ...WidgetOption) *demov1alpha1.Widget {
	w := &demov1alpha1.Widget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	for _, opt := range opts {
		opt(w)
	}

	return w
}

// WithPluginRef sets the pluginRef field on the Widget spec.
func WithPluginRef(ref string) WidgetOption {
	return func(w *demov1alpha1.Widget) {
		w.Spec.PluginRef = ref
	}
}

// PluginConfig creates a PluginConfig resource with the given name, namespace, and setting.
func PluginConfig(name, namespace, setting string) *demov1alpha1.PluginConfig {
	return &demov1alpha1.PluginConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: demov1alpha1.PluginConfigSpec{
			Setting: setting,
		},
	}
}
