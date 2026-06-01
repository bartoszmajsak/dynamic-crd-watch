package dynamicwatch_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"
)

func TestConditionTypeFromCRDName(t *testing.T) {
	tests := []struct {
		crdName  string
		expected string
	}{
		{"httproutes.gateway.networking.k8s.io", "HTTPRoutesAvailable"},
		{"leaderworkersets.leaderworkerset.x-k8s.io", "LeaderworkersetsAvailable"},
		{"scaledobjects.keda.sh", "ScaledobjectsAvailable"},
		{"configmaps.example.com", "ConfigmapsAvailable"},
		{"widgets.demo.example.com", "WidgetsAvailable"},
		{"pluginconfigs.demo.example.com", "PluginconfigsAvailable"},
		{"apiservices.apiregistration.k8s.io", "APIServicesAvailable"},
		{"dnszones.example.com", "DNSZonesAvailable"},
		{"grpcroutes.gateway.networking.k8s.io", "GRPCRoutesAvailable"},
		{"tlsroutes.gateway.networking.k8s.io", "TLSRoutesAvailable"},
		{"tcproutes.gateway.networking.k8s.io", "TCPRoutesAvailable"},
	}

	for _, tt := range tests {
		t.Run(tt.crdName, func(t *testing.T) {
			// Create a watcher with the crdName and check what condition type comes out.
			w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap](tt.crdName, &fakeCache{}, context.Background())
			c := w.Condition()
			if c.Type != tt.expected {
				t.Errorf("conditionType for %q: got %q, want %q", tt.crdName, c.Type, tt.expected)
			}
		})
	}
}

func TestCondition_Active(t *testing.T) {
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("widgets.example.com", &fakeCache{}, context.Background())
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	c := w.Condition()

	if c.Status != metav1.ConditionTrue {
		t.Errorf("expected ConditionTrue, got %s", c.Status)
	}
	if c.Reason != "Ready" {
		t.Errorf("expected reason Ready, got %s", c.Reason)
	}
	if c.Type != "WidgetsAvailable" {
		t.Errorf("expected type WidgetsAvailable, got %s", c.Type)
	}
}

func TestCondition_CRDNotFound(t *testing.T) {
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("widgets.example.com", &fakeCache{}, context.Background())

	c := w.Condition()

	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected ConditionFalse, got %s", c.Status)
	}
	if c.Reason != "CRDNotFound" {
		t.Errorf("expected reason CRDNotFound, got %s", c.Reason)
	}
}

func TestCondition_Syncing(t *testing.T) {
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("widgets.example.com", &fakeCache{}, context.Background())
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetWatching(w, true)

	c := w.Condition()

	if c.Status != metav1.ConditionFalse {
		t.Errorf("expected ConditionFalse, got %s", c.Status)
	}
	if c.Reason != "Syncing" {
		t.Errorf("expected reason Syncing, got %s", c.Reason)
	}
}

func TestHandle_Condition_Delegates(t *testing.T) {
	w := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("widgets.example.com", &fakeCache{}, context.Background())
	dynamicwatch.SetCRDExists(w, true)
	dynamicwatch.SetActive(w, true)

	h := dynamicwatch.NewTestHandle(w)
	c := h.Condition()

	if c.Status != metav1.ConditionTrue {
		t.Errorf("expected ConditionTrue through handle, got %s", c.Status)
	}
}

func TestRegistry_Conditions(t *testing.T) {
	reg := dynamicwatch.NewTestRegistry()
	ctx := context.Background()

	w1 := dynamicwatch.NewTestWatcher[*corev1.ConfigMap]("widgets.example.com", &fakeCache{}, ctx)
	dynamicwatch.SetCRDExists(w1, true)
	dynamicwatch.SetActive(w1, true)
	dynamicwatch.RegistryInsert(reg, "widgets.example.com", w1)

	w2 := dynamicwatch.NewTestWatcher[*corev1.Secret]("secrets.example.com", &fakeCache{}, ctx)
	dynamicwatch.RegistryInsert(reg, "secrets.example.com", w2)

	conditions := reg.Conditions()

	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conditions))
	}

	byType := make(map[string]metav1.Condition, len(conditions))
	for _, c := range conditions {
		byType[c.Type] = c
	}

	if c, ok := byType["WidgetsAvailable"]; !ok {
		t.Error("missing WidgetsAvailable condition")
	} else if c.Status != metav1.ConditionTrue {
		t.Errorf("WidgetsAvailable: expected True, got %s", c.Status)
	}

	if c, ok := byType["SecretsAvailable"]; !ok {
		t.Error("missing SecretsAvailable condition")
	} else if c.Status != metav1.ConditionFalse {
		t.Errorf("SecretsAvailable: expected False, got %s", c.Status)
	}
}
