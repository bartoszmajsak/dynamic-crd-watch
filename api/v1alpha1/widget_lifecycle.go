package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionPluginReady = "PluginReady"

	ReasonPluginCRDNotAvailable = "PluginCRDNotAvailable"
	ReasonPluginApplied         = "PluginApplied"
	ReasonPluginNotFound        = "PluginNotFound"
)

func (w *Widget) MarkPluginApplied(message string) {
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonPluginApplied,
		Message:            message,
		ObservedGeneration: w.Generation,
	})
}

func (w *Widget) MarkPluginCRDNotAvailable() {
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonPluginCRDNotAvailable,
		Message:            "PluginConfig CRD is not installed",
		ObservedGeneration: w.Generation,
	})
}

func (w *Widget) MarkPluginNotFound() {
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               ConditionPluginReady,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonPluginNotFound,
		Message:            "Referenced PluginConfig does not exist",
		ObservedGeneration: w.Generation,
	})
}

func (w *Widget) RemovePluginCondition() {
	meta.RemoveStatusCondition(&w.Status.Conditions, ConditionPluginReady)
}

func (w *Widget) HasPluginCondition() bool {
	return meta.FindStatusCondition(w.Status.Conditions, ConditionPluginReady) != nil
}
