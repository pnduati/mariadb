package admission

import (
	"fmt"
	"sync"

	"github.com/appscode/go/log"
	"github.com/appscode/go/types"
	"github.com/google/uuid"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	cs "github.com/kubedb/apimachinery/client/clientset/versioned"
	"github.com/pkg/errors"
	admission "k8s.io/api/admission/v1beta1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kutil "kmodules.xyz/client-go"
	core_util "kmodules.xyz/client-go/core/v1"
	meta_util "kmodules.xyz/client-go/meta"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
	hookapi "kmodules.xyz/webhook-runtime/admission/v1beta1"
)

type MariaDBMutator struct {
	client      kubernetes.Interface
	extClient   cs.Interface
	lock        sync.RWMutex
	initialized bool
}

var _ hookapi.AdmissionHook = &MariaDBMutator{}

func (a *MariaDBMutator) Resource() (plural schema.GroupVersionResource, singular string) {
	return schema.GroupVersionResource{
			Group:    "mutators.kubedb.com",
			Version:  "v1alpha1",
			Resource: "mariadbmutators",
		},
		"mariadbmutator"
}

func (a *MariaDBMutator) Initialize(config *rest.Config, stopCh <-chan struct{}) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.initialized = true

	var err error
	if a.client, err = kubernetes.NewForConfig(config); err != nil {
		return err
	}
	if a.extClient, err = cs.NewForConfig(config); err != nil {
		return err
	}
	return err
}

func (a *MariaDBMutator) Admit(req *admission.AdmissionRequest) *admission.AdmissionResponse {
	status := &admission.AdmissionResponse{}

	// N.B.: No Mutating for delete
	if (req.Operation != admission.Create && req.Operation != admission.Update) ||
		len(req.SubResource) != 0 ||
		req.Kind.Group != api.SchemeGroupVersion.Group ||
		req.Kind.Kind != api.ResourceKindMariaDB {
		status.Allowed = true
		return status
	}

	a.lock.RLock()
	defer a.lock.RUnlock()
	if !a.initialized {
		return hookapi.StatusUninitialized()
	}
	obj, err := meta_util.UnmarshalFromJSON(req.Object.Raw, api.SchemeGroupVersion)
	if err != nil {
		return hookapi.StatusBadRequest(err)
	}
	mariadbMod, err := setDefaultValues(a.client, a.extClient, obj.(*api.MariaDB).DeepCopy())
	if err != nil {
		return hookapi.StatusForbidden(err)
	} else if mariadbMod != nil {
		patch, err := meta_util.CreateJSONPatch(req.Object.Raw, mariadbMod)
		if err != nil {
			return hookapi.StatusInternalServerError(err)
		}
		status.Patch = patch
		patchType := admission.PatchTypeJSONPatch
		status.PatchType = &patchType
	}

	status.Allowed = true
	return status
}

// setDefaultValues provides the defaulting that is performed in mutating stage of creating/updating a MariaDB database
func setDefaultValues(client kubernetes.Interface, extClient cs.Interface, mariadb *api.MariaDB) (runtime.Object, error) {
	if mariadb.Spec.Version == "" {
		return nil, errors.New(`'spec.version' is missing`)
	}

	if mariadb.Spec.Topology != nil && mariadb.Spec.Topology.Mode != nil &&
		*mariadb.Spec.Topology.Mode == api.MariaDBClusterModeGroup {
		if mariadb.Spec.Topology.Group == nil {
			mariadb.Spec.Topology.Group = &api.MariaDBGroupSpec{}
		}

		if mariadb.Spec.Topology.Group.Name == "" {
			grName, err := uuid.NewRandom()
			if err != nil {
				return nil, errors.New("failed to generate a new group name")
			}
			mariadb.Spec.Topology.Group.Name = grName.String()
		}

		if mariadb.Spec.Topology.Group.BaseServerID == nil {
			mariadb.Spec.Topology.Group.BaseServerID = types.UIntP(api.MariaDBDefaultBaseServerID)
		}
	}

	mariadb.SetDefaults()

	if err := setDefaultsFromDormantDB(extClient, mariadb); err != nil {
		return nil, err
	}

	// If monitoring spec is given without port,
	// set default Listening port
	setMonitoringPort(mariadb)

	return mariadb, nil
}

// setDefaultsFromDormantDB takes values from Similar Dormant Database
func setDefaultsFromDormantDB(extClient cs.Interface, mariadb *api.MariaDB) error {
	// Check if DormantDatabase exists or not
	dormantDb, err := extClient.KubedbV1alpha1().DormantDatabases(mariadb.Namespace).Get(mariadb.Name, metav1.GetOptions{})
	if err != nil {
		if !kerr.IsNotFound(err) {
			return err
		}
		return nil
	}

	// Check DatabaseKind
	if value, _ := meta_util.GetStringValue(dormantDb.Labels, api.LabelDatabaseKind); value != api.ResourceKindMariaDB {
		return errors.New(fmt.Sprintf(`invalid MariaDB: "%v/%v". Exists DormantDatabase "%v/%v" of different Kind`, mariadb.Namespace, mariadb.Name, dormantDb.Namespace, dormantDb.Name))
	}

	// Check Origin Spec
	ddbOriginSpec := dormantDb.Spec.Origin.Spec.MariaDB
	ddbOriginSpec.SetDefaults()

	// If DatabaseSecret of new object is not given,
	// Take dormantDatabaseSecretName
	if mariadb.Spec.DatabaseSecret == nil {
		mariadb.Spec.DatabaseSecret = ddbOriginSpec.DatabaseSecret
	}

	// If Monitoring Spec of new object is not given,
	// Take Monitoring Settings from Dormant
	if mariadb.Spec.Monitor == nil {
		mariadb.Spec.Monitor = ddbOriginSpec.Monitor
	} else {
		ddbOriginSpec.Monitor = mariadb.Spec.Monitor
	}

	// If Backup Scheduler of new object is not given,
	// Take Backup Scheduler Settings from Dormant
	if mariadb.Spec.BackupSchedule == nil {
		mariadb.Spec.BackupSchedule = ddbOriginSpec.BackupSchedule
	} else {
		ddbOriginSpec.BackupSchedule = mariadb.Spec.BackupSchedule
	}

	// Skip checking UpdateStrategy
	ddbOriginSpec.UpdateStrategy = mariadb.Spec.UpdateStrategy

	// Skip checking TerminationPolicy
	ddbOriginSpec.TerminationPolicy = mariadb.Spec.TerminationPolicy

	if !meta_util.Equal(ddbOriginSpec, &mariadb.Spec) {
		diff := meta_util.Diff(ddbOriginSpec, &mariadb.Spec)
		log.Errorf("mariadb spec mismatches with OriginSpec in DormantDatabases. Diff: %v", diff)
		return errors.New(fmt.Sprintf("mariadb spec mismatches with OriginSpec in DormantDatabases. Diff: %v", diff))
	}

	if _, err := meta_util.GetString(mariadb.Annotations, api.AnnotationInitialized); err == kutil.ErrNotFound &&
		mariadb.Spec.Init != nil &&
		mariadb.Spec.Init.SnapshotSource != nil {
		mariadb.Annotations = core_util.UpsertMap(mariadb.Annotations, map[string]string{
			api.AnnotationInitialized: "",
		})
	}

	// Delete  Matching dormantDatabase in Controller

	return nil
}

// Assign Default Monitoring Port if MonitoringSpec Exists
// and the AgentVendor is Prometheus.
func setMonitoringPort(mariadb *api.MariaDB) {
	if mariadb.Spec.Monitor != nil &&
		mariadb.GetMonitoringVendor() == mona.VendorPrometheus {
		if mariadb.Spec.Monitor.Prometheus == nil {
			mariadb.Spec.Monitor.Prometheus = &mona.PrometheusSpec{}
		}
		if mariadb.Spec.Monitor.Prometheus.Port == 0 {
			mariadb.Spec.Monitor.Prometheus.Port = api.PrometheusExporterPortNumber
		}
	}
}
