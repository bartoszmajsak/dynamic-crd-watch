package dynamicwatch //nolint:testpackage // White-box tests for unexported helpers.

import (
	"context"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

func TestStripCRDSpec_RemovesSpec_PreservesStatusAndMetadata(t *testing.T) {
	crd := &apiextensionsv1.CustomResourceDefinition{
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "example.com",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "widgets",
			},
		},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{
			Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
			},
		},
	}
	crd.Name = "widgets.example.com"

	result, err := stripCRDSpec(crd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stripped, ok := result.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		t.Fatalf("expected *CRD, got %T", result)
	}

	// Spec should be zeroed.
	if stripped.Spec.Group != "" {
		t.Errorf("expected empty spec.group, got %q", stripped.Spec.Group)
	}

	// Metadata should be preserved.
	if stripped.Name != "widgets.example.com" {
		t.Errorf("expected name preserved, got %q", stripped.Name)
	}

	// Status should be preserved - this is critical for isCRDEstablished.
	if len(stripped.Status.Conditions) != 1 {
		t.Fatalf("expected 1 status condition, got %d", len(stripped.Status.Conditions))
	}

	if !isCRDEstablished(stripped) {
		t.Error("expected isCRDEstablished to return true after stripCRDSpec")
	}
}

func TestStripCRDSpec_NonCRDObject_PassesThrough(t *testing.T) {
	obj := "not a CRD"
	result, err := stripCRDSpec(obj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != obj {
		t.Error("expected non-CRD object to pass through unchanged")
	}
}

func TestStart_CalledTwice_ReturnsError(t *testing.T) {
	// The guard fires before accessing crdCache, so a nil crdCache is fine.
	w := &Watcher[client.Object]{
		startSource: func(_ source.SyncingSource) error { return nil },
	}

	err := w.Start(context.Background(), nil)
	if err == nil {
		t.Error("expected error on double Start")
	}
}

func TestIsCRDEstablished(t *testing.T) {
	tests := []struct {
		name       string
		conditions []apiextensionsv1.CustomResourceDefinitionCondition
		want       bool
	}{
		{
			name:       "no conditions",
			conditions: nil,
			want:       false,
		},
		{
			name: "established true",
			conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
			},
			want: true,
		},
		{
			name: "established false",
			conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionFalse},
			},
			want: false,
		},
		{
			name: "other conditions only",
			conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
				{Type: apiextensionsv1.NamesAccepted, Status: apiextensionsv1.ConditionTrue},
			},
			want: false,
		},
		{
			name:       "empty conditions slice",
			conditions: []apiextensionsv1.CustomResourceDefinitionCondition{},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crd := &apiextensionsv1.CustomResourceDefinition{
				Status: apiextensionsv1.CustomResourceDefinitionStatus{
					Conditions: tt.conditions,
				},
			}

			if got := isCRDEstablished(crd); got != tt.want {
				t.Errorf("isCRDEstablished() = %v, want %v", got, tt.want)
			}
		})
	}
}
