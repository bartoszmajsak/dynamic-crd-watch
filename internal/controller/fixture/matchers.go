package fixture

import (
	"fmt"

	"github.com/onsi/gomega/types"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HaveCondition returns a Gomega matcher that checks whether a slice of
// metav1.Condition contains a condition with the given type and status.
func HaveCondition(conditionType string, status metav1.ConditionStatus) types.GomegaMatcher {
	return &conditionMatcher{
		conditionType: conditionType,
		status:        status,
	}
}

// HaveConditionWithReason returns a Gomega matcher that checks whether a slice of
// metav1.Condition contains a condition with the given type, status, and reason.
func HaveConditionWithReason(conditionType string, status metav1.ConditionStatus, reason string) types.GomegaMatcher {
	return &conditionMatcher{
		conditionType: conditionType,
		status:        status,
		reason:        reason,
		checkReason:   true,
	}
}

// NotHaveCondition returns a Gomega matcher that checks whether a slice of
// metav1.Condition does NOT contain a condition with the given type.
func NotHaveCondition(conditionType string) types.GomegaMatcher {
	return &noConditionMatcher{
		conditionType: conditionType,
	}
}

type conditionMatcher struct {
	conditionType string
	status        metav1.ConditionStatus
	reason        string
	checkReason   bool
}

func (m *conditionMatcher) Match(actual any) (bool, error) {
	conditions, ok := actual.([]metav1.Condition)
	if !ok {
		return false, fmt.Errorf("expected []metav1.Condition, got %T", actual)
	}

	cond := meta.FindStatusCondition(conditions, m.conditionType)
	if cond == nil {
		return false, nil
	}

	if cond.Status != m.status {
		return false, nil
	}

	if m.checkReason && cond.Reason != m.reason {
		return false, nil
	}

	return true, nil
}

func (m *conditionMatcher) FailureMessage(actual any) string {
	if m.checkReason {
		return fmt.Sprintf("expected conditions to contain %s=%s (reason=%s), got:\n%v",
			m.conditionType, m.status, m.reason, actual)
	}

	return fmt.Sprintf("expected conditions to contain %s=%s, got:\n%v",
		m.conditionType, m.status, actual)
}

func (m *conditionMatcher) NegatedFailureMessage(actual any) string {
	if m.checkReason {
		return fmt.Sprintf("expected conditions NOT to contain %s=%s (reason=%s), got:\n%v",
			m.conditionType, m.status, m.reason, actual)
	}

	return fmt.Sprintf("expected conditions NOT to contain %s=%s, got:\n%v",
		m.conditionType, m.status, actual)
}

type noConditionMatcher struct {
	conditionType string
}

func (m *noConditionMatcher) Match(actual any) (bool, error) {
	conditions, ok := actual.([]metav1.Condition)
	if !ok {
		return false, fmt.Errorf("expected []metav1.Condition, got %T", actual)
	}

	return meta.FindStatusCondition(conditions, m.conditionType) == nil, nil
}

func (m *noConditionMatcher) FailureMessage(actual any) string {
	return fmt.Sprintf("expected conditions NOT to contain %s, got:\n%v",
		m.conditionType, actual)
}

func (m *noConditionMatcher) NegatedFailureMessage(actual any) string {
	return fmt.Sprintf("expected conditions to contain %s, got:\n%v",
		m.conditionType, actual)
}
