package controller_test

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("dynamicwatch.Build", func() {

	It("succeeds with a plain manager cache (no special configuration required)", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: k8sClient.Scheme(),
			Metrics: metricsserver.Options{
				BindAddress: "0",
			},
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = dynamicwatch.For[*demov1alpha1.PluginConfig](mgr, "pluginconfigs.demo.example.com").
			EnqueueOnObjectChange(noopObjectMapper).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).NotTo(HaveOccurred())
	})
})

func noopObjectMapper(_ context.Context, _ *demov1alpha1.PluginConfig) []reconcile.Request {
	return nil
}

func noopRequeueAll(_ context.Context) []reconcile.Request {
	return nil
}
