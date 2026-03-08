package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WidgetSpec defines the desired state of Widget.
type WidgetSpec struct {
	// pluginRef is an optional reference to a PluginConfig resource name.
	// When set, the controller will attempt to read the referenced PluginConfig
	// and apply its settings. If the PluginConfig CRD is not installed,
	// the Widget will report PluginReady=False.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PluginRef string `json:"pluginRef,omitempty"`
}

// WidgetStatus defines the observed state of Widget.
type WidgetStatus struct {
	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Widget resource.
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Widget is the Schema for the widgets API.
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +optional
	Spec WidgetSpec `json:"spec,omitempty"`
	// +optional
	Status WidgetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WidgetList contains a list of Widget.
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`

	Items []Widget `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Widget{}, &WidgetList{})
}
