package controller_test

import (
	"context"
	"os"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bartoszmajsak/dynamic-watch-poc/internal/controller"
	"github.com/bartoszmajsak/dynamic-watch-poc/internal/controller/fixture"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Widget Controller", func() {

	const namespace = "default"

	Context("without pluginRef", func() {

		It("should reconcile without setting PluginReady condition", func(ctx SpecContext) {
			widget := fixture.Widget("no-plugin", namespace)
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
			}).WithContext(ctx).Should(Succeed())

			Consistently(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.NotHaveCondition(controller.ConditionPluginReady),
				)
			}).WithContext(ctx).WithTimeout(2 * time.Second).Should(Succeed())
		})
	})

	Context("with pluginRef but PluginConfig CRD not installed", func() {

		It("should set PluginReady=False with reason PluginCRDNotAvailable", func(ctx SpecContext) {
			widget := fixture.Widget("wants-plugin", namespace, fixture.WithPluginRef("my-plugin"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionFalse,
						controller.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})
	})

	Context("dynamic CRD lifecycle", Ordered, func() {

		var crd *apiextensionsv1.CustomResourceDefinition

		AfterEach(func(ctx SpecContext) {
			if crd == nil {
				return
			}

			_ = client.IgnoreNotFound(k8sClient.Delete(ctx, crd))

			Eventually(func(g Gomega, ctx context.Context) {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(crd), &apiextensionsv1.CustomResourceDefinition{})
				g.Expect(err).To(HaveOccurred(), "CRD should be deleted")
			}).WithContext(ctx).Should(Succeed())

			crd = nil
		})

		installPluginConfigCRD := func(ctx context.Context) {
			crd = loadCRDFromFile(pluginConfigCRDPath)
			Expect(k8sClient.Create(ctx, crd)).To(Succeed())

			// Wait for the CRD to be established before proceeding.
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(crd), crd)).To(Succeed())
				established := false
				for _, c := range crd.Status.Conditions {
					if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
						established = true
					}
				}
				g.Expect(established).To(BeTrue(), "CRD should be established")
			}).WithContext(ctx).Should(Succeed())
		}

		removePluginConfigCRD := func(ctx context.Context) {
			Expect(k8sClient.Delete(ctx, crd)).To(Succeed())

			Eventually(func(g Gomega, ctx context.Context) {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(crd), crd)
				g.Expect(err).To(HaveOccurred())
			}).WithContext(ctx).Should(Succeed())
		}

		It("should dynamically register watch when CRD is installed", func(ctx SpecContext) {
			widget := fixture.Widget("dynamic-watch", namespace, fixture.WithPluginRef("my-plugin"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("initially reporting PluginCRDNotAvailable")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionFalse,
						controller.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())

			By("installing PluginConfig CRD at runtime")
			installPluginConfigCRD(ctx)

			By("creating the referenced PluginConfig")
			plugin := fixture.PluginConfig("my-plugin", namespace, "hello-from-plugin")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, plugin))).To(Succeed())
			})

			By("eventually transitioning to PluginReady=True")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionTrue,
						controller.ReasonPluginApplied,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should clean up informer when CRD is removed", func(ctx SpecContext) {
			widget := fixture.Widget("watch-removal", namespace, fixture.WithPluginRef("removal-plugin"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("installing CRD and creating PluginConfig")
			installPluginConfigCRD(ctx)

			plugin := fixture.PluginConfig("removal-plugin", namespace, "will-be-removed")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			By("waiting for PluginReady=True")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(controller.ConditionPluginReady, metav1.ConditionTrue),
				)
			}).WithContext(ctx).Should(Succeed())

			By("removing the PluginConfig CRD")
			removePluginConfigCRD(ctx)

			By("eventually transitioning back to PluginCRDNotAvailable")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionFalse,
						controller.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should set PluginNotFound when CRD exists but referenced PluginConfig does not", func(ctx SpecContext) {
			By("installing PluginConfig CRD")
			installPluginConfigCRD(ctx)

			widget := fixture.Widget("missing-plugin", namespace, fixture.WithPluginRef("nonexistent"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("expecting PluginReady=False with reason PluginNotFound")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionFalse,
						controller.ReasonPluginNotFound,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should handle full add/remove/re-add cycle", func(ctx SpecContext) {
			widget := fixture.Widget("full-cycle", namespace, fixture.WithPluginRef("cycle-plugin"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("step 1: install CRD, create plugin — should become ready")
			installPluginConfigCRD(ctx)

			plugin := fixture.PluginConfig("cycle-plugin", namespace, "first-install")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(controller.ConditionPluginReady, metav1.ConditionTrue),
				)
			}).WithContext(ctx).Should(Succeed())

			By("step 2: remove CRD — should become not available")
			removePluginConfigCRD(ctx)

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionFalse,
						controller.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())

			By("step 3: re-install CRD, create plugin again — should become ready again")
			installPluginConfigCRD(ctx)

			plugin = fixture.PluginConfig("cycle-plugin", namespace, "second-install")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(controller.ConditionPluginReady, metav1.ConditionTrue),
				)
			}).WithContext(ctx).Should(Succeed())
		})
	})

	Context("pluginRef lifecycle on existing Widget", func() {

		It("should set condition when pluginRef is added to existing Widget", func(ctx SpecContext) {
			widget := fixture.Widget("add-ref-later", namespace)
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("initially having no PluginReady condition")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
			}).WithContext(ctx).Should(Succeed())

			Consistently(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.NotHaveCondition(controller.ConditionPluginReady),
				)
			}).WithContext(ctx).WithTimeout(2 * time.Second).Should(Succeed())

			By("updating Widget to add a pluginRef")
			Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget); err != nil {
					return err
				}
				widget.Spec.PluginRef = "some-plugin"

				return k8sClient.Update(ctx, widget)
			})).To(Succeed())

			By("eventually getting PluginReady=False since CRD is not installed")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						controller.ConditionPluginReady,
						metav1.ConditionFalse,
						controller.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should clear condition when pluginRef is removed from Widget", func(ctx SpecContext) {
			widget := fixture.Widget("remove-ref", namespace, fixture.WithPluginRef("phantom"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("initially having PluginReady=False")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(controller.ConditionPluginReady, metav1.ConditionFalse),
				)
			}).WithContext(ctx).Should(Succeed())

			By("removing pluginRef from Widget")
			Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget); err != nil {
					return err
				}
				widget.Spec.PluginRef = ""

				return k8sClient.Update(ctx, widget)
			})).To(Succeed())

			By("eventually clearing the PluginReady condition")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.NotHaveCondition(controller.ConditionPluginReady),
				)
			}).WithContext(ctx).Should(Succeed())
		})
	})
})

func loadCRDFromFile(path string) *apiextensionsv1.CustomResourceDefinition {
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())

	crd := &apiextensionsv1.CustomResourceDefinition{}
	Expect(yaml.UnmarshalStrict(data, crd)).To(Succeed())

	return crd
}
