package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PluginConfigSpec defines the desired state of PluginConfig.
type PluginConfigSpec struct {
	// setting is a simple configuration value applied to the Widget.
	// +optional
	Setting string `json:"setting,omitempty"`
}

// +kubebuilder:object:root=true

// PluginConfig is the Schema for the pluginconfigs API.
// This CRD is optional — it may or may not be installed in the cluster.
type PluginConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`

	Spec PluginConfigSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// PluginConfigList contains a list of PluginConfig.
type PluginConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`

	Items []PluginConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PluginConfig{}, &PluginConfigList{})
}
