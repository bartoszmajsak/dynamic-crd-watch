package controller //nolint:testpackage // White-box test for unexported mergeResults.

import (
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

func TestMergeResults(t *testing.T) {
	tests := []struct {
		name    string
		results []ctrl.Result
		want    time.Duration
	}{
		{
			name:    "both zero",
			results: []ctrl.Result{{}, {}},
			want:    0,
		},
		{
			name:    "first non-zero",
			results: []ctrl.Result{{RequeueAfter: 5 * time.Second}, {}},
			want:    5 * time.Second,
		},
		{
			name:    "second non-zero",
			results: []ctrl.Result{{}, {RequeueAfter: 3 * time.Second}},
			want:    3 * time.Second,
		},
		{
			name:    "both non-zero picks shorter",
			results: []ctrl.Result{{RequeueAfter: 5 * time.Second}, {RequeueAfter: 2 * time.Second}},
			want:    2 * time.Second,
		},
		{
			name:    "single result",
			results: []ctrl.Result{{RequeueAfter: time.Second}},
			want:    time.Second,
		},
		{
			name:    "empty",
			results: nil,
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeResults(tt.results...)
			if got.RequeueAfter != tt.want {
				t.Errorf("mergeResults() RequeueAfter = %v, want %v", got.RequeueAfter, tt.want)
			}
		})
	}
}
