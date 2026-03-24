package dynamicwatch

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	watcherActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dynamicwatch",
			Name:      "active",
			Help:      "Whether the dynamic watch is currently active and readable (1=active, 0=inactive).",
		},
		[]string{"crd"},
	)

	watcherTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dynamicwatch",
			Name:      "state_transitions_total",
			Help:      "Total number of watcher state transitions.",
		},
		[]string{"crd", "transition"},
	)
)

func init() {
	metrics.Registry.MustRegister(watcherActive, watcherTransitions)
}
