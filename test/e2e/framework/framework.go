package framework

import (
	"github.com/appscode/go/crypto/rand"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	cs "github.com/kubedb/apimachinery/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ka "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	appcat_cs "kmodules.xyz/custom-resources/client/clientset/versioned/typed/appcatalog/v1alpha1"
)

var (
	DockerRegistry     = "kubedbci"
	SelfHostedOperator = false
	DBCatalogName      = "8.0-v2"
	DBVersion          = "8.0-v2"
	DBToolsTag         = "8.0-v3"
	ExporterTag        = "v0.11.0"
)

type Framework struct {
	restConfig       *rest.Config
	kubeClient       kubernetes.Interface
	extClient        cs.Interface
	kaClient         ka.Interface
	appCatalogClient appcat_cs.AppcatalogV1alpha1Interface
	namespace        string
	name             string
	StorageClass     string
}

func New(
	restConfig *rest.Config,
	kubeClient kubernetes.Interface,
	extClient cs.Interface,
	kaClient ka.Interface,
	appCatalogClient appcat_cs.AppcatalogV1alpha1Interface,
	storageClass string,
) *Framework {
	return &Framework{
		restConfig:       restConfig,
		kubeClient:       kubeClient,
		extClient:        extClient,
		kaClient:         kaClient,
		appCatalogClient: appCatalogClient,
		name:             "mariadb-operator",
		namespace:        rand.WithUniqSuffix(api.ResourceSingularMariaDB),
		StorageClass:     storageClass,
	}
}

func (f *Framework) Invoke() *Invocation {
	return &Invocation{
		Framework: f,
		app:       rand.WithUniqSuffix("mariadb-e2e"),
	}
}

func (fi *Invocation) App() string {
	return fi.app
}

func (fi *Invocation) ExtClient() cs.Interface {
	return fi.extClient
}

type Invocation struct {
	*Framework
	app string
}
