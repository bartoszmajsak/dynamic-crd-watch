package dynamicwatch

import (
	"fmt"
	"strings"
	"unicode"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition returns a [metav1.Condition] reflecting the watcher's current
// state. The condition type is either auto-derived from the CRD name or
// set explicitly via [WatcherBuilder.WithConditionType].
//
// The caller can pass an optional conditionType to override the stored one
// for this call only. If empty, the stored condition type is used.
func (w *Watcher[T]) Condition() metav1.Condition {
	s := w.Status()

	c := metav1.Condition{
		Type:   w.conditionType,
		Reason: string(s.Reason),
	}

	if s.Available {
		c.Status = metav1.ConditionTrue
		c.Message = fmt.Sprintf("Watch for %s is active", w.crdName)
	} else {
		c.Status = metav1.ConditionFalse
		c.Message = conditionMessage(w.crdName, s.Reason)
	}

	return c
}

// Conditions returns a [metav1.Condition] for every registered watcher.
// The conditions are keyed by the watcher's condition type. This is useful
// for bulk-updating a CR's status with all optional CRD states at once.
func (r *Registry) Conditions() []metav1.Condition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	conditions := make([]metav1.Condition, 0, len(r.watchers))
	for _, w := range r.watchers {
		if cw, ok := w.(interface{ Condition() metav1.Condition }); ok {
			conditions = append(conditions, cw.Condition())
		}
	}

	return conditions
}

func conditionMessage(crdName string, reason WatcherReason) string {
	switch reason {
	case ReasonCRDNotFound:
		return fmt.Sprintf("CRD %s is not installed", crdName)
	case ReasonSyncing:
		return fmt.Sprintf("Watch for %s is syncing", crdName)
	case ReasonPending:
		return fmt.Sprintf("Watch for %s is pending registration", crdName)
	case ReasonNotStarted:
		return fmt.Sprintf("Watcher for %s has not started", crdName)
	default:
		return fmt.Sprintf("Watch for %s is in state %s", crdName, reason)
	}
}

// conditionTypeFromCRDName derives a PascalCase condition type from a CRD
// name. It takes the plural part (before the first dot) and converts it
// to PascalCase, appending "Available".
//
// Examples:
//
//	httproutes.gateway.networking.k8s.io -> HTTPRoutesAvailable
//	leaderworkersets.leaderworkerset.x-k8s.io -> LeaderWorkerSetsAvailable
//	scaledobjects.keda.sh -> ScaledObjectsAvailable
func conditionTypeFromCRDName(crdName string) string {
	plural, _, _ := strings.Cut(crdName, ".")

	return toPascalCase(plural) + "Available"
}

// toPascalCase converts a lowercase word to PascalCase, treating transitions
// from lowercase to uppercase in common abbreviations as word boundaries.
// Single all-lowercase words get their first letter capitalized.
//
// For CRD plurals that are compound words (e.g. "leaderworkersets",
// "scaledobjects", "httproutes"), this uses a simple heuristic:
// known prefixes and abbreviations are uppercased.
func toPascalCase(s string) string {
	if s == "" {
		return s
	}

	// Handle known abbreviations at the start.
	for _, prefix := range []string{"http", "grpc", "tcp", "tls", "dns", "api", "crd"} {
		if strings.HasPrefix(s, prefix) {
			rest := s[len(prefix):]
			if rest == "" {
				return strings.ToUpper(prefix)
			}

			return strings.ToUpper(prefix) + toPascalCase(rest)
		}
	}

	// Capitalize first letter, leave the rest as-is.
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])

	return string(runes)
}
