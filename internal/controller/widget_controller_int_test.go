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

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
	"github.com/bartoszmajsak/dynamic-watch-poc/testing/fixture"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Widget Controller", func() {

	const namespace = "default"

	// CRD install/remove helpers use directClient (uncached) because the manager's
	// cache has ReaderFailOnMissingInformer: true and CRD informers live only in
	// each Watcher's dedicated cache, not the main cache.
	installCRD := func(ctx context.Context, path string) *apiextensionsv1.CustomResourceDefinition {
		crd := loadCRDFromFile(path)
		Expect(directClient.Create(ctx, crd)).To(Succeed())

		Eventually(func(g Gomega, ctx context.Context) {
			g.Expect(directClient.Get(ctx, client.ObjectKeyFromObject(crd), crd)).To(Succeed())
			established := false
			for _, c := range crd.Status.Conditions {
				if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
					established = true
				}
			}
			g.Expect(established).To(BeTrue(), "CRD should be established")
		}).WithContext(ctx).Should(Succeed())

		return crd
	}

	removeCRD := func(ctx context.Context, crd *apiextensionsv1.CustomResourceDefinition) {
		Expect(directClient.Delete(ctx, crd)).To(Succeed())

		Eventually(func(g Gomega, ctx context.Context) {
			err := directClient.Get(ctx, client.ObjectKeyFromObject(crd), crd)
			g.Expect(err).To(HaveOccurred())
		}).WithContext(ctx).Should(Succeed())
	}

	deferCRDCleanup := func(crd *apiextensionsv1.CustomResourceDefinition) {
		DeferCleanup(func(ctx SpecContext) {
			_ = client.IgnoreNotFound(directClient.Delete(ctx, crd))
			Eventually(func(g Gomega, ctx context.Context) {
				err := directClient.Get(ctx, client.ObjectKeyFromObject(crd), &apiextensionsv1.CustomResourceDefinition{})
				g.Expect(err).To(HaveOccurred(), "CRD should be deleted")
			}).WithContext(ctx).Should(Succeed())
		})
	}

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
					fixture.NotHaveCondition(demov1alpha1.ConditionPluginReady),
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
				g.Expect(widget.Status.ObservedGeneration).To(Equal(widget.Generation))
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})
	})

	Context("dynamic CRD lifecycle", func() {

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
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())

			By("installing PluginConfig CRD at runtime")
			crd := installCRD(ctx, pluginConfigCRDPath)
			deferCRDCleanup(crd)

			By("creating the referenced PluginConfig")
			plugin := fixture.PluginConfig("my-plugin", namespace, "hello-from-plugin")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, plugin))).To(Succeed())
			})

			By("eventually transitioning to PluginReady=True")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.ObservedGeneration).To(Equal(widget.Generation))
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionTrue,
						demov1alpha1.ReasonPluginApplied,
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
			crd := installCRD(ctx, pluginConfigCRDPath)

			plugin := fixture.PluginConfig("removal-plugin", namespace, "will-be-removed")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			By("waiting for PluginReady=True")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(demov1alpha1.ConditionPluginReady, metav1.ConditionTrue),
				)
			}).WithContext(ctx).Should(Succeed())

			By("removing the PluginConfig CRD")
			removeCRD(ctx, crd)

			By("eventually transitioning back to PluginCRDNotAvailable")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should set PluginNotFound when CRD exists but referenced PluginConfig does not", func(ctx SpecContext) {
			By("installing PluginConfig CRD")
			crd := installCRD(ctx, pluginConfigCRDPath)
			deferCRDCleanup(crd)

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
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginNotFound,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should update Widget condition when PluginConfig setting changes", func(ctx SpecContext) {
			By("installing PluginConfig CRD")
			crd := installCRD(ctx, pluginConfigCRDPath)
			deferCRDCleanup(crd)

			plugin := fixture.PluginConfig("update-test", namespace, "original-setting")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, plugin))).To(Succeed())
			})

			widget := fixture.Widget("watches-update", namespace, fixture.WithPluginRef("update-test"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("waiting for PluginReady=True with original setting")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionTrue,
						demov1alpha1.ReasonPluginApplied,
					),
				)
			}).WithContext(ctx).Should(Succeed())

			By("updating PluginConfig setting")
			Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(plugin), plugin); err != nil {
					return err
				}
				plugin.Spec.Setting = "updated-setting"

				return k8sClient.Update(ctx, plugin)
			})).To(Succeed())

			By("eventually reflecting the new setting in the condition message")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithMessage(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionTrue,
						"updated-setting",
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

			By("step 1: install CRD, create plugin - should become ready")
			crd := installCRD(ctx, pluginConfigCRDPath)

			plugin := fixture.PluginConfig("cycle-plugin", namespace, "first-install")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(demov1alpha1.ConditionPluginReady, metav1.ConditionTrue),
				)
			}).WithContext(ctx).Should(Succeed())

			By("step 2: remove CRD - should become not available")
			removeCRD(ctx, crd)

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())

			By("step 3: re-install CRD, create plugin again - should become ready again")
			crd = installCRD(ctx, pluginConfigCRDPath)
			deferCRDCleanup(crd)

			plugin = fixture.PluginConfig("cycle-plugin", namespace, "second-install")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(demov1alpha1.ConditionPluginReady, metav1.ConditionTrue),
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
					fixture.NotHaveCondition(demov1alpha1.ConditionPluginReady),
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
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginCRDNotAvailable,
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
					fixture.HaveCondition(demov1alpha1.ConditionPluginReady, metav1.ConditionFalse),
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
					fixture.NotHaveCondition(demov1alpha1.ConditionPluginReady),
				)
			}).WithContext(ctx).Should(Succeed())
		})
	})

	Context("Theme CRD lifecycle", func() {

		It("should set ThemeReady=False when themeRef set but Theme CRD not installed", func(ctx SpecContext) {
			widget := fixture.Widget("wants-theme", namespace, fixture.WithThemeRef("my-theme"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonThemeCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should set ThemeNotFound when CRD exists but referenced Theme does not", func(ctx SpecContext) {
			By("installing Theme CRD")
			crd := installCRD(ctx, themeCRDPath)
			deferCRDCleanup(crd)

			widget := fixture.Widget("missing-theme", namespace, fixture.WithThemeRef("nonexistent"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("expecting ThemeReady=False with reason ThemeNotFound")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.ObservedGeneration).To(Equal(widget.Generation))
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonThemeNotFound,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should set ThemeReady=True when Theme CRD installed and Theme exists", func(ctx SpecContext) {
			widget := fixture.Widget("theme-ready", namespace, fixture.WithThemeRef("dark"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("initially reporting ThemeCRDNotAvailable")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonThemeCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())

			By("installing Theme CRD")
			crd := installCRD(ctx, themeCRDPath)
			deferCRDCleanup(crd)

			By("creating the referenced Theme")
			theme := fixture.Theme("dark", namespace, "solarized-dark")
			Expect(k8sClient.Create(ctx, theme)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, theme))).To(Succeed())
			})

			By("eventually transitioning to ThemeReady=True")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionTrue,
						demov1alpha1.ReasonThemeApplied,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})

		It("should clean up Theme informer when Theme CRD is removed", func(ctx SpecContext) {
			widget := fixture.Widget("theme-removal", namespace, fixture.WithThemeRef("removable"))
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("installing Theme CRD and creating Theme")
			crd := installCRD(ctx, themeCRDPath)

			theme := fixture.Theme("removable", namespace, "will-go-away")
			Expect(k8sClient.Create(ctx, theme)).To(Succeed())

			By("waiting for ThemeReady=True")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveCondition(demov1alpha1.ConditionThemeReady, metav1.ConditionTrue),
				)
			}).WithContext(ctx).Should(Succeed())

			By("removing the Theme CRD")
			removeCRD(ctx, crd)

			By("eventually transitioning back to ThemeCRDNotAvailable")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonThemeCRDNotAvailable,
					),
				)
			}).WithContext(ctx).Should(Succeed())
		})
	})

	Context("both pluginRef and themeRef", func() {

		It("should track both conditions independently", func(ctx SpecContext) {
			widget := fixture.Widget("dual-ref", namespace,
				fixture.WithPluginRef("dual-plugin"),
				fixture.WithThemeRef("dual-theme"),
			)
			Expect(k8sClient.Create(ctx, widget)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, widget))).To(Succeed())
			})

			By("initially both conditions should be CRDNotAvailable")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(And(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonPluginCRDNotAvailable,
					),
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonThemeCRDNotAvailable,
					),
				))
			}).WithContext(ctx).Should(Succeed())

			By("installing only PluginConfig CRD and creating the plugin")
			pluginCRD := installCRD(ctx, pluginConfigCRDPath)
			deferCRDCleanup(pluginCRD)

			plugin := fixture.PluginConfig("dual-plugin", namespace, "plugin-active")
			Expect(k8sClient.Create(ctx, plugin)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, plugin))).To(Succeed())
			})

			By("PluginReady=True while ThemeReady remains False")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.Conditions).To(And(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionTrue,
						demov1alpha1.ReasonPluginApplied,
					),
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionFalse,
						demov1alpha1.ReasonThemeCRDNotAvailable,
					),
				))
			}).WithContext(ctx).Should(Succeed())

			By("installing Theme CRD and creating the theme")
			themeCRD := installCRD(ctx, themeCRDPath)
			deferCRDCleanup(themeCRD)

			theme := fixture.Theme("dual-theme", namespace, "ocean-blue")
			Expect(k8sClient.Create(ctx, theme)).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, theme))).To(Succeed())
			})

			By("both conditions should be True with correct observedGeneration")
			Eventually(func(g Gomega, ctx context.Context) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(widget), widget)).To(Succeed())
				g.Expect(widget.Status.ObservedGeneration).To(Equal(widget.Generation))
				g.Expect(widget.Status.Conditions).To(And(
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionPluginReady,
						metav1.ConditionTrue,
						demov1alpha1.ReasonPluginApplied,
					),
					fixture.HaveConditionWithReason(
						demov1alpha1.ConditionThemeReady,
						metav1.ConditionTrue,
						demov1alpha1.ReasonThemeApplied,
					),
				))
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
