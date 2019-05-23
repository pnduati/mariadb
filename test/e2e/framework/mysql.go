package framework

import (
	"fmt"
	"time"

	"github.com/appscode/go/crypto/rand"
	jsonTypes "github.com/appscode/go/encoding/json/types"
	"github.com/appscode/go/types"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha1/util"
	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	JobPvcStorageSize = "2Gi"
	DBPvcStorageSize  = "1Gi"
)

func (f *Invocation) MariaDB() *api.MariaDB {
	return &api.MariaDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix("mariadb"),
			Namespace: f.namespace,
			Labels: map[string]string{
				"app": f.app,
			},
		},
		Spec: api.MariaDBSpec{
			Version: jsonTypes.StrYo(DBVersion),
			Storage: &core.PersistentVolumeClaimSpec{
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse(DBPvcStorageSize),
					},
				},
				StorageClassName: types.StringP(f.StorageClass),
			},
		},
	}
}

func (f *Invocation) MariaDBGroup() *api.MariaDB {
	mariadb := f.MariaDB()
	mariadb.Spec.Replicas = types.Int32P(api.MariaDBDefaultGroupSize)
	clusterMode := api.MariaDBClusterModeGroup
	mariadb.Spec.Topology = &api.MariaDBClusterTopology{
		Mode: &clusterMode,
		Group: &api.MariaDBGroupSpec{
			Name:         "dc002fc3-c412-4d18-b1d4-66c1fbfbbc9b",
			BaseServerID: types.UIntP(api.MariaDBDefaultBaseServerID),
		},
	}

	return mariadb
}

func (f *Framework) CreateMariaDB(obj *api.MariaDB) error {
	_, err := f.extClient.KubedbV1alpha1().MariaDBs(obj.Namespace).Create(obj)
	return err
}

func (f *Framework) GetMariaDB(meta metav1.ObjectMeta) (*api.MariaDB, error) {
	return f.extClient.KubedbV1alpha1().MariaDBs(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
}

func (f *Framework) PatchMariaDB(meta metav1.ObjectMeta, transform func(*api.MariaDB) *api.MariaDB) (*api.MariaDB, error) {
	mariadb, err := f.extClient.KubedbV1alpha1().MariaDBs(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	mariadb, _, err = util.PatchMariaDB(f.extClient.KubedbV1alpha1(), mariadb, transform)
	return mariadb, err
}

func (f *Framework) DeleteMariaDB(meta metav1.ObjectMeta) error {
	return f.extClient.KubedbV1alpha1().MariaDBs(meta.Namespace).Delete(meta.Name, &metav1.DeleteOptions{})
}

func (f *Framework) EventuallyMariaDB(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			_, err := f.extClient.KubedbV1alpha1().MariaDBs(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
			if err != nil {
				if kerr.IsNotFound(err) {
					return false
				}
				Expect(err).NotTo(HaveOccurred())
			}
			return true
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) EventuallyMariaDBRunning(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			mariadb, err := f.extClient.KubedbV1alpha1().MariaDBs(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			return mariadb.Status.Phase == api.DatabasePhaseRunning
		},
		time.Minute*15,
		time.Second*5,
	)
}

func (f *Framework) CleanMariaDB() {
	mariadbList, err := f.extClient.KubedbV1alpha1().MariaDBs(f.namespace).List(metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, e := range mariadbList.Items {
		if _, _, err := util.PatchMariaDB(f.extClient.KubedbV1alpha1(), &e, func(in *api.MariaDB) *api.MariaDB {
			in.ObjectMeta.Finalizers = nil
			in.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
			return in
		}); err != nil {
			fmt.Printf("error Patching MariaDB. error: %v", err)
		}
	}
	if err := f.extClient.KubedbV1alpha1().MariaDBs(f.namespace).DeleteCollection(deleteInForeground(), metav1.ListOptions{}); err != nil {
		fmt.Printf("error in deletion of MariaDB. Error: %v", err)
	}
}
