package controller_test

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// managerDeploymentName must match kustomize output (namePrefix + "controller-manager").
const managerDeploymentName = "dynamic-watch-poc-controller-manager"

func waitForManagerReady(ctx context.Context) {
	deploy := &appsv1.Deployment{}
	key := client.ObjectKey{Name: managerDeploymentName, Namespace: managerNamespace}

	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, key, deploy)).To(Succeed())
		g.Expect(deploy.Status.ReadyReplicas).To(
			BeNumerically(">=", 1),
			"manager deployment should have at least 1 ready replica",
		)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
}

func collectKubeDiagnostics(ctx context.Context) {
	fmt.Fprintln(GinkgoWriter, "\n=== Cluster Diagnostics ===")

	var widgets demov1alpha1.WidgetList
	if err := k8sClient.List(ctx, &widgets); err == nil {
		fmt.Fprintln(GinkgoWriter, "\n--- Widgets ---")

		for i := range widgets.Items {
			w := &widgets.Items[i]
			fmt.Fprintf(GinkgoWriter, "  %s/%s: pluginRef=%q themeRef=%q conditions=%v\n",
				w.Namespace, w.Name, w.Spec.PluginRef, w.Spec.ThemeRef, w.Status.Conditions)
		}
	}

	var pods corev1.PodList
	if err := k8sClient.List(ctx, &pods,
		client.InNamespace(managerNamespace),
		client.MatchingLabels{"control-plane": "controller-manager"},
	); err == nil {
		fmt.Fprintln(GinkgoWriter, "\n--- Manager Pods ---")

		for i := range pods.Items {
			p := &pods.Items[i]
			fmt.Fprintf(GinkgoWriter, "  %s: phase=%s\n", p.Name, p.Status.Phase)

			for _, cs := range p.Status.ContainerStatuses {
				fmt.Fprintf(GinkgoWriter, "    container %s: ready=%v restarts=%d\n",
					cs.Name, cs.Ready, cs.RestartCount)

				if cs.State.Waiting != nil {
					fmt.Fprintf(GinkgoWriter, "      waiting: %s — %s\n",
						cs.State.Waiting.Reason, cs.State.Waiting.Message)
				}

				if cs.State.Terminated != nil {
					fmt.Fprintf(GinkgoWriter, "      terminated: %s (exit %d)\n",
						cs.State.Terminated.Reason, cs.State.Terminated.ExitCode)
				}
			}
		}
	}

	var events corev1.EventList
	if err := k8sClient.List(ctx, &events, client.InNamespace("default")); err == nil {
		fmt.Fprintln(GinkgoWriter, "\n--- Events (default) ---")

		for i := range events.Items {
			e := &events.Items[i]
			fmt.Fprintf(GinkgoWriter, "  %s %s/%s: %s\n",
				e.Type, e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Message)
		}
	}

	if err := k8sClient.List(ctx, &events, client.InNamespace(managerNamespace)); err == nil {
		fmt.Fprintln(GinkgoWriter, "\n--- Events (manager) ---")

		for i := range events.Items {
			e := &events.Items[i]
			fmt.Fprintf(GinkgoWriter, "  %s %s/%s: %s\n",
				e.Type, e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Message)
		}
	}
}
