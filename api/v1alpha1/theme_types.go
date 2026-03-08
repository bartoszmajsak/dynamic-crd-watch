package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ThemeSpec defines the desired state of Theme.
type ThemeSpec struct {
	// colorScheme is a simple color scheme value applied to the Widget.
	// +optional
	ColorScheme string `json:"colorScheme,omitempty"`
}

// +kubebuilder:object:root=true

// Theme is the Schema for the themes API.
// This CRD is optional — it may or may not be installed in the cluster.
type Theme struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`

	Spec ThemeSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ThemeList contains a list of Theme.
type ThemeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`

	Items []Theme `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Theme{}, &ThemeList{})
}
