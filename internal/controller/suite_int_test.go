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
	"github.com/bartoszmajsak/dynamic-watch-poc/testing/cluster"
	"github.com/bartoszmajsak/dynamic-watch-poc/testing/fixture"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client

	// directClient bypasses the manager's cache. Required for CRD operations
	// because CRD informers live in each Watcher's dedicated cache, not the
	// manager's main cache (which has ReaderFailOnMissingInformer: true).
	directClient client.Client

	deployedManager bool

	// Paths to optional CRD YAMLs — used to install/remove them dynamically in tests.
	pluginConfigCRDPath string
	themeCRDPath        string
)

// Three test modes, controlled by environment variables:
//
//   - envtest (default): embedded apiserver + in-process manager.
//     Fast, no cluster required. Run with: make test
//
//   - USE_EXISTING_CLUSTER: real cluster apiserver + in-process manager.
//     Tests real CRD lifecycle, etcd behaviour, admission webhooks.
//     Run with: make test-int (requires: make kind-create)
//
//   - USE_EXISTING_CLUSTER + DEPLOYED_MANAGER: real cluster + deployed manager pod.
//     Full e2e: validates container image, kustomize manifests, RBAC.
//     Run with: make test-e2e (requires: make kind-create)
func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	if os.Getenv("USE_EXISTING_CLUSTER") != "" {
		deployedManager = os.Getenv("DEPLOYED_MANAGER") != ""

		SetDefaultEventuallyTimeout(30 * time.Second)
		SetDefaultEventuallyPollingInterval(500 * time.Millisecond)
	} else {
		SetDefaultEventuallyTimeout(10 * time.Second)
		SetDefaultEventuallyPollingInterval(250 * time.Millisecond)
	}

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // package-level var, not nested

	Expect(demov1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(apiextensionsv1.AddToScheme(scheme.Scheme)).To(Succeed())

	root := fixture.ProjectRoot()
	pluginConfigCRDPath = filepath.Join(root, "config", "crd", "bases", "demo.example.com_pluginconfigs.yaml")
	themeCRDPath = filepath.Join(root, "config", "crd", "bases", "demo.example.com_themes.yaml")

	switch {
	case deployedManager:
		setupDeployedManager()
	case os.Getenv("USE_EXISTING_CLUSTER") != "":
		setupExistingCluster(root)
	default:
		setupEnvtest(root)
	}
})

// ReportAfterEach runs even on interrupt/timeout (unlike AfterEach),
// ensuring diagnostics are collected when tests hang or get Ctrl+C'd.
var _ = ReportAfterEach(func(report SpecReport) {
	if deployedManager && report.Failed() {
		cluster.CollectKubeDiagnostics(ctx, k8sClient)
	}
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()

	if testEnv != nil {
		Eventually(func() error {
			return testEnv.Stop()
		}, time.Minute, time.Second).Should(Succeed())
	}
})

// setupDeployedManager connects to an existing cluster where the manager
// is already running as a pod (docker-build → docker-push → deploy).
func setupDeployedManager() {
	By("connecting to cluster from kubeconfig (deployed manager)")
	cfg = ctrl.GetConfigOrDie()

	var err error
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	directClient = k8sClient // no manager cache in deployed mode

	By("verifying manager deployment is ready")
	cluster.WaitForManagerReady(ctx, k8sClient)
}

// setupExistingCluster connects to a real cluster via kubeconfig and starts
// the manager in-process. This exercises real CRD lifecycle and etcd behaviour
// without requiring docker build or pod deployment.
func setupExistingCluster(root string) {
	By("connecting to cluster from kubeconfig (in-process manager)")
	useExisting := true
	testEnv = &envtest.Environment{
		UseExistingCluster: &useExisting,
		// Only install Widget CRD at startup. PluginConfig CRD is installed dynamically in tests.
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths:              []string{filepath.Join(root, "config", "crd", "bases", "demo.example.com_widgets.yaml")},
			ErrorIfPathMissing: true,
		},
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	startInProcessManager()
}

// setupEnvtest starts an embedded apiserver via envtest and runs the manager in-process.
func setupEnvtest(root string) {
	_, err := os.Stat(pluginConfigCRDPath)
	Expect(err).NotTo(HaveOccurred(), "PluginConfig CRD YAML must exist — run 'make manifests'")
	_, err = os.Stat(themeCRDPath)
	Expect(err).NotTo(HaveOccurred(), "Theme CRD YAML must exist — run 'make manifests'")

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

	startInProcessManager()
}

func startInProcessManager() {
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

	var err2 error
	directClient, err2 = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err2).NotTo(HaveOccurred())
}

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
