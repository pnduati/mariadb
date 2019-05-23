package framework

import (
	"fmt"

	api "github.com/kubedb/apimachinery/apis/catalog/v1alpha1"
	. "github.com/onsi/gomega"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (i *Invocation) MariaDBVersion() *api.MariaDBVersion {
	return &api.MariaDBVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name: DBVersion,
			Labels: map[string]string{
				"app": i.app,
			},
		},
		Spec: api.MariaDBVersionSpec{
			Version: DBVersion,
			DB: api.MariaDBVersionDatabase{
				Image: fmt.Sprintf("%s/mariadb:%s", DockerRegistry, DBVersion),
			},
			Exporter: api.MariaDBVersionExporter{
				Image: fmt.Sprintf("%s/mariadbd-exporter:%s", DockerRegistry, ExporterTag),
			},
			Tools: api.MariaDBVersionTools{
				Image: fmt.Sprintf("%s/mariadb-tools:%s", DockerRegistry, DBToolsTag),
			},
			InitContainer: api.MariaDBVersionInitContainer{
				Image: "kubedb/busybox",
			},
			PodSecurityPolicies: api.MariaDBVersionPodSecurityPolicy{
				SnapshotterPolicyName: "mariadb-snapshot",
				DatabasePolicyName:    "mariadb-db",
			},
		},
	}
}

func (f *Framework) CreateMariaDBVersion(obj *api.MariaDBVersion) error {
	_, err := f.extClient.CatalogV1alpha1().MariaDBVersions().Create(obj)
	if err != nil && kerr.IsAlreadyExists(err) {
		e2 := f.extClient.CatalogV1alpha1().MariaDBVersions().Delete(obj.Name, &metav1.DeleteOptions{})
		Expect(e2).NotTo(HaveOccurred())
		_, e2 = f.extClient.CatalogV1alpha1().MariaDBVersions().Create(obj)
		return e2
	}
	return nil
}

func (f *Framework) DeleteMariaDBVersion(meta metav1.ObjectMeta) error {
	return f.extClient.CatalogV1alpha1().MariaDBVersions().Delete(meta.Name, &metav1.DeleteOptions{})
}
