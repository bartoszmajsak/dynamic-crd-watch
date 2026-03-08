package v1alpha1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types for Widget status.
const (
	// ConditionPluginReady indicates whether the referenced PluginConfig
	// has been successfully applied to the Widget.
	ConditionPluginReady = "PluginReady"
)

// Reasons for the PluginReady condition.
const (
	// ReasonPluginCRDNotAvailable means the PluginConfig CRD is not installed in the cluster.
	ReasonPluginCRDNotAvailable = "PluginCRDNotAvailable"
	// ReasonPluginApplied means the referenced PluginConfig was found and applied.
	ReasonPluginApplied = "PluginApplied"
	// ReasonPluginNotFound means the PluginConfig CRD exists but the referenced resource does not.
	ReasonPluginNotFound = "PluginNotFound"
)

// MarkPluginApplied sets PluginReady=True with the plugin's setting as the message.
func (w *Widget) MarkPluginApplied(message string) {
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonPluginApplied,
		Message:            message,
		ObservedGeneration: w.Generation,
	})
}

// MarkPluginCRDNotAvailable sets PluginReady=False because the PluginConfig CRD
// is not installed in the cluster.
func (w *Widget) MarkPluginCRDNotAvailable() {
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonPluginCRDNotAvailable,
		Message:            "PluginConfig CRD is not installed",
		ObservedGeneration: w.Generation,
	})
}

// MarkPluginNotFound sets PluginReady=False because the referenced PluginConfig
// resource does not exist.
func (w *Widget) MarkPluginNotFound(pluginRef string) {
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonPluginNotFound,
		Message:            fmt.Sprintf("PluginConfig %q not found", pluginRef),
		ObservedGeneration: w.Generation,
	})
}

// RemovePluginCondition removes the PluginReady condition entirely.
func (w *Widget) RemovePluginCondition() {
	meta.RemoveStatusCondition(&w.Status.Conditions, ConditionPluginReady)
}

// HasPluginCondition reports whether the PluginReady condition is present.
func (w *Widget) HasPluginCondition() bool {
	return meta.FindStatusCondition(w.Status.Conditions, ConditionPluginReady) != nil
}
