package controller_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	demov1alpha1 "github.com/bartoszmajsak/dynamic-watch-poc/api/v1alpha1"
	"github.com/bartoszmajsak/dynamic-watch-poc/internal/controller"
	"github.com/bartoszmajsak/dynamic-watch-poc/internal/controller/fixture"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client

	// Path to the PluginConfig CRD YAML — used to install/remove it dynamically in tests.
	pluginConfigCRDPath string
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	SetDefaultEventuallyTimeout(10 * time.Second)
	SetDefaultEventuallyPollingInterval(250 * time.Millisecond)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // package-level var, not nested

	Expect(demov1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(apiextensionsv1.AddToScheme(scheme.Scheme)).To(Succeed())

	root := fixture.ProjectRoot()

	pluginConfigCRDPath = filepath.Join(root, "config", "crd", "bases", "demo.example.com_pluginconfigs.yaml")
	_, err := os.Stat(pluginConfigCRDPath)
	Expect(err).NotTo(HaveOccurred(), "PluginConfig CRD YAML must exist — run 'make manifests'")

	By("bootstrapping test environment with Widget CRD only")
	testEnv = &envtest.Environment{
		// Only install Widget CRD at startup. PluginConfig CRD is installed dynamically in tests.
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths:              []string{filepath.Join(root, "config", "crd", "bases", "demo.example.com_widgets.yaml")},
			ErrorIfPathMissing: true,
		},
	}

	if dir := firstEnvTestBinaryDir(root); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Cache: cache.Options{
			// Prevent r.Get from auto-creating informers for GVKs whose informer was removed.
			// Without this, reading a PluginConfig after its informer was removed would block
			// forever waiting for a new informer to sync against a non-existent CRD.
			ReaderFailOnMissingInformer: true,
		},
		Metrics: metricsserver.Options{
			BindAddress: "0", // disable metrics in tests
		},
	})
	Expect(err).NotTo(HaveOccurred())

	reconciler := &controller.WidgetReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}
	Expect(reconciler.SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()

	k8sClient = mgr.GetClient()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	Eventually(func() error {
		return testEnv.Stop()
	}, time.Minute, time.Second).Should(Succeed())
})

// firstEnvTestBinaryDir finds envtest binaries for IDE-based test runs
// where KUBEBUILDER_ASSETS is not set via Makefile.
func firstEnvTestBinaryDir(root string) string {
	basePath := filepath.Join(root, "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}

	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}

	return ""
}
