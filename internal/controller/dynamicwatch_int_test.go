package controller_test

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
	"github.com/bartoszmajsak/dynamic-watch-poc/pkg/dynamicwatch"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// unregisteredType is a minimal client.Object not added to any scheme.
// Used to test the defensive GVK check in Build().
type unregisteredType struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`
}

func (u *unregisteredType) DeepCopyObject() runtime.Object   { return u }
func (u *unregisteredType) GetObjectKind() schema.ObjectKind { return &u.TypeMeta }

var _ = Describe("dynamicwatch.Build", func() {

	newMgr := func() ctrl.Manager {
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: k8sClient.Scheme(),
			Metrics: metricsserver.Options{
				BindAddress: "0",
			},
		})
		Expect(err).NotTo(HaveOccurred())

		return mgr
	}

	It("succeeds with WithEventHandler", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).NotTo(HaveOccurred())
	})

	It("succeeds with EnqueueForOwner", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			EnqueueForOwner(&demov1alpha1.Widget{}).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).NotTo(HaveOccurred())
	})

	It("succeeds with EnqueueForOwner and WithPredicates", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			EnqueueForOwner(&demov1alpha1.Widget{}, handler.OnlyControllerOwner()).
			WithPredicates(acceptAllPluginConfigs).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).NotTo(HaveOccurred())
	})

	It("succeeds with WithEventHandler and WithPredicates", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			WithPredicates(acceptAllPluginConfigs).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects when neither WithEventHandler nor EnqueueForOwner is set", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("WithEventHandler or EnqueueForOwner is required")))
	})

	It("rejects when both WithEventHandler and EnqueueForOwner are set", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			EnqueueForOwner(&demov1alpha1.Widget{}).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("mutually exclusive")))
	})

	It("rejects when owner type is not registered in the scheme", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
			EnqueueForOwner(&unregisteredType{}).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("deriving GVK for owner type")))
	})

	It("panics when EnqueueForOwner is called with nil", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		Expect(func() {
			dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
				EnqueueForOwner(nil)
		}).To(PanicWith(ContainSubstring("nil ownerType")))
	})

	It("panics when WithEventHandler is called with nil", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		Expect(func() {
			dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "pluginconfigs.demo.example.com").
				WithEventHandler(nil)
		}).To(PanicWith(ContainSubstring("nil handler")))
	})

	It("panics when For is called with nil manager", func() {
		Expect(func() {
			dynamicwatch.For[*demov1alpha1.PluginConfig](nil, "pluginconfigs.demo.example.com")
		}).To(PanicWith(ContainSubstring("nil manager")))
	})

	It("rejects invalid CRD name - empty string", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("crdName is required")))
	})

	It("rejects invalid CRD name - bare plural without group", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "widgets").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("invalid CRD name")))
		Expect(err).To(MatchError(ContainSubstring("expected format: <plural>.<group>")))
	})

	It("rejects invalid CRD name - trailing dot", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), "widgets.").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("invalid CRD name")))
		Expect(err).To(MatchError(ContainSubstring("expected format: <plural>.<group>")))
	})

	It("rejects invalid CRD name - leading dot", func() {
		if deployedManager {
			Skip("requires in-process manager")
		}

		_, err := dynamicwatch.For[*demov1alpha1.PluginConfig](newMgr(), ".example.com").
			WithEventHandler(handler.TypedEnqueueRequestsFromMapFunc(noopObjectMapper)).
			EnqueueOnCRDChange(noopRequeueAll).
			Build()
		Expect(err).To(MatchError(ContainSubstring("invalid CRD name")))
		Expect(err).To(MatchError(ContainSubstring("expected format: <plural>.<group>")))
	})
})

// acceptAllPluginConfigs is a properly typed predicate for use in WithPredicates tests.
// Generic predicates like predicate.GenerationChangedPredicate are typed for
// client.Object and don't satisfy TypedPredicate[*PluginConfig].
var acceptAllPluginConfigs = predicate.NewTypedPredicateFuncs(func(_ *demov1alpha1.PluginConfig) bool {
	return true
})

func noopObjectMapper(_ context.Context, _ *demov1alpha1.PluginConfig) []reconcile.Request {
	return nil
}

func noopRequeueAll(_ context.Context) []reconcile.Request {
	return nil
}
