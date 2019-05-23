package admission

import (
	"net/http"
	"testing"

	"github.com/appscode/go/types"
	catalog "github.com/kubedb/apimachinery/apis/catalog/v1alpha1"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	extFake "github.com/kubedb/apimachinery/client/clientset/versioned/fake"
	"github.com/kubedb/apimachinery/client/clientset/versioned/scheme"
	admission "k8s.io/api/admission/v1beta1"
	apps "k8s.io/api/apps/v1"
	authenticationV1 "k8s.io/api/authentication/v1"
	core "k8s.io/api/core/v1"
	storageV1beta1 "k8s.io/api/storage/v1beta1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientSetScheme "k8s.io/client-go/kubernetes/scheme"
	"kmodules.xyz/client-go/meta"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
)

func init() {
	scheme.AddToScheme(clientSetScheme.Scheme)
}

var requestKind = metaV1.GroupVersionKind{
	Group:   api.SchemeGroupVersion.Group,
	Version: api.SchemeGroupVersion.Version,
	Kind:    api.ResourceKindMariaDB,
}

func TestMariaDBValidator_Admit(t *testing.T) {
	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			validator := MariaDBValidator{}

			validator.initialized = true
			validator.extClient = extFake.NewSimpleClientset(
				&catalog.MariaDBVersion{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "8.0",
					},
					Spec: catalog.MariaDBVersionSpec{
						Version: "8.0.0",
					},
				},
				&catalog.MariaDBVersion{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "5.6",
					},
					Spec: catalog.MariaDBVersionSpec{
						Version: "5.6",
					},
				},
				&catalog.MariaDBVersion{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "5.7.25",
					},
					Spec: catalog.MariaDBVersionSpec{
						Version: "5.7.25",
					},
				},
			)
			validator.client = fake.NewSimpleClientset(
				&core.Secret{
					ObjectMeta: metaV1.ObjectMeta{
						Name:      "foo-auth",
						Namespace: "default",
					},
				},
				&storageV1beta1.StorageClass{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "standard",
					},
				},
			)

			objJS, err := meta.MarshalToJson(&c.object, api.SchemeGroupVersion)
			if err != nil {
				panic(err)
			}
			oldObjJS, err := meta.MarshalToJson(&c.oldObject, api.SchemeGroupVersion)
			if err != nil {
				panic(err)
			}

			req := new(admission.AdmissionRequest)

			req.Kind = c.kind
			req.Name = c.objectName
			req.Namespace = c.namespace
			req.Operation = c.operation
			req.UserInfo = authenticationV1.UserInfo{}
			req.Object.Raw = objJS
			req.OldObject.Raw = oldObjJS

			if c.heatUp {
				if _, err := validator.extClient.KubedbV1alpha1().MariaDBs(c.namespace).Create(&c.object); err != nil && !kerr.IsAlreadyExists(err) {
					t.Errorf(err.Error())
				}
			}
			if c.operation == admission.Delete {
				req.Object = runtime.RawExtension{}
			}
			if c.operation != admission.Update {
				req.OldObject = runtime.RawExtension{}
			}

			response := validator.Admit(req)
			if c.result == true {
				if response.Allowed != true {
					t.Errorf("expected: 'Allowed=true'. but got response: %v", response)
				}
			} else if c.result == false {
				if response.Allowed == true || response.Result.Code == http.StatusInternalServerError {
					t.Errorf("expected: 'Allowed=false', but got response: %v", response)
				}
			}
		})
	}

}

var cases = []struct {
	testName   string
	kind       metaV1.GroupVersionKind
	objectName string
	namespace  string
	operation  admission.Operation
	object     api.MariaDB
	oldObject  api.MariaDB
	heatUp     bool
	result     bool
}{
	{"Create Valid MariaDB",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleMariaDB(),
		api.MariaDB{},
		false,
		true,
	},
	{"Create Invalid MariaDB",
		requestKind,
		"foo",
		"default",
		admission.Create,
		getAwkwardMariaDB(),
		api.MariaDB{},
		false,
		false,
	},
	{"Edit MariaDB Spec.DatabaseSecret with Existing Secret",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editExistingSecret(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit MariaDB Spec.DatabaseSecret with non Existing Secret",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editNonExistingSecret(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit Status",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editStatus(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit Spec.Monitor",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecMonitor(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit Invalid Spec.Monitor",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecInvalidMonitor(sampleMariaDB()),
		sampleMariaDB(),
		false,
		false,
	},
	{"Edit Spec.TerminationPolicy",
		requestKind,
		"foo",
		"default",
		admission.Update,
		pauseDatabase(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Delete MariaDB when Spec.TerminationPolicy=DoNotTerminate",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		sampleMariaDB(),
		api.MariaDB{},
		true,
		false,
	},
	{"Delete MariaDB when Spec.TerminationPolicy=Pause",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		pauseDatabase(sampleMariaDB()),
		api.MariaDB{},
		true,
		true,
	},
	{"Delete Non Existing MariaDB",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		api.MariaDB{},
		api.MariaDB{},
		false,
		true,
	},

	// For MariaDB Group Replication
	{"Create valid group",
		requestKind,
		"foo",
		"default",
		admission.Create,
		validGroup(sampleMariaDB()),
		api.MariaDB{},
		false,
		true,
	},
	{"Create group with '.spec.topology.mode' not set",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithClusterModeNotSet(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with invalid '.spec.topology.mode'",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithInvalidClusterMode(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with single replica",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithSingleReplica(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with replicas more than max group size",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithOverReplicas(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with unsupported MariaDB server version",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithUnsupportedServerVersion(),
		api.MariaDB{},
		false,
		true,
	},
	{"Create group with Non-tri formatted MariaDB server version",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithNonTriFormattedServerVersion(),
		api.MariaDB{},
		false,
		true,
	},
	{"Create group with empty group name",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithEmptyGroupName(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with invalid group name",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithInvalidGroupName(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with baseServerID 0",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithBaseServerIDZero(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create group with baseServerID exceeded max limit",
		requestKind,
		"foo",
		"default",
		admission.Create,
		groupWithBaseServerIDExceededMaxLimit(),
		api.MariaDB{},
		false,
		false,
	},
}

func sampleMariaDB() api.MariaDB {
	return api.MariaDB{
		TypeMeta: metaV1.TypeMeta{
			Kind:       api.ResourceKindMariaDB,
			APIVersion: api.SchemeGroupVersion.String(),
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				api.LabelDatabaseKind: api.ResourceKindMariaDB,
			},
		},
		Spec: api.MariaDBSpec{
			Version:     "8.0",
			Replicas:    types.Int32P(1),
			StorageType: api.StorageTypeDurable,
			Storage: &core.PersistentVolumeClaimSpec{
				StorageClassName: types.StringP("standard"),
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse("100Mi"),
					},
				},
			},
			Init: &api.InitSpec{
				ScriptSource: &api.ScriptSourceSpec{
					VolumeSource: core.VolumeSource{
						GitRepo: &core.GitRepoVolumeSource{
							Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
							Directory:  ".",
						},
					},
				},
			},
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
			TerminationPolicy: api.TerminationPolicyDoNotTerminate,
		},
	}
}

func getAwkwardMariaDB() api.MariaDB {
	mariadb := sampleMariaDB()
	mariadb.Spec.Version = "3.0"
	return mariadb
}

func editExistingSecret(old api.MariaDB) api.MariaDB {
	old.Spec.DatabaseSecret = &core.SecretVolumeSource{
		SecretName: "foo-auth",
	}
	return old
}

func editNonExistingSecret(old api.MariaDB) api.MariaDB {
	old.Spec.DatabaseSecret = &core.SecretVolumeSource{
		SecretName: "foo-auth-fused",
	}
	return old
}

func editStatus(old api.MariaDB) api.MariaDB {
	old.Status = api.MariaDBStatus{
		Phase: api.DatabasePhaseCreating,
	}
	return old
}

func editSpecMonitor(old api.MariaDB) api.MariaDB {
	old.Spec.Monitor = &mona.AgentSpec{
		Agent: mona.AgentPrometheusBuiltin,
		Prometheus: &mona.PrometheusSpec{
			Port: 1289,
		},
	}
	return old
}

// should be failed because more fields required for COreOS Monitoring
func editSpecInvalidMonitor(old api.MariaDB) api.MariaDB {
	old.Spec.Monitor = &mona.AgentSpec{
		Agent: mona.AgentCoreOSPrometheus,
	}
	return old
}

func pauseDatabase(old api.MariaDB) api.MariaDB {
	old.Spec.TerminationPolicy = api.TerminationPolicyPause
	return old
}

func validGroup(old api.MariaDB) api.MariaDB {
	old.Spec.Version = api.MariaDBGRRecommendedVersion
	old.Spec.Replicas = types.Int32P(api.MariaDBDefaultGroupSize)
	clusterMode := api.MariaDBClusterModeGroup
	old.Spec.Topology = &api.MariaDBClusterTopology{
		Mode: &clusterMode,
		Group: &api.MariaDBGroupSpec{
			Name:         "dc002fc3-c412-4d18-b1d4-66c1fbfbbc9b",
			BaseServerID: types.UIntP(api.MariaDBDefaultBaseServerID),
		},
	}

	return old
}

func groupWithClusterModeNotSet() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Topology.Mode = nil

	return old
}

func groupWithInvalidClusterMode() api.MariaDB {
	old := validGroup(sampleMariaDB())
	gr := api.MariaDBClusterMode("groupReplication")
	old.Spec.Topology.Mode = &gr

	return old
}

func groupWithSingleReplica() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Replicas = types.Int32P(1)

	return old
}

func groupWithOverReplicas() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Replicas = types.Int32P(api.MariaDBMaxGroupMembers + 1)

	return old
}

func groupWithUnsupportedServerVersion() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Version = "8.0"

	return old
}

func groupWithNonTriFormattedServerVersion() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Version = "5.6"

	return old
}

func groupWithEmptyGroupName() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Topology.Group.Name = ""

	return old
}

func groupWithInvalidGroupName() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Topology.Group.Name = "a-a-a-a-a"

	return old
}

func groupWithBaseServerIDZero() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Topology.Group.BaseServerID = types.UIntP(0)

	return old
}

func groupWithBaseServerIDExceededMaxLimit() api.MariaDB {
	old := validGroup(sampleMariaDB())
	old.Spec.Topology.Group.BaseServerID = types.UIntP(api.MariaDBMaxBaseServerID + 1)

	return old
}
