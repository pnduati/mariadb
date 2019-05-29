package admission

import (
	"fmt"
	"strings"
	"sync"

	"github.com/appscode/go/log"
	"github.com/coreos/go-semver/semver"
	"github.com/google/uuid"
	cat_api "github.com/kubedb/apimachinery/apis/catalog/v1alpha1"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	cs "github.com/kubedb/apimachinery/client/clientset/versioned"
	amv "github.com/kubedb/apimachinery/pkg/validator"
	"github.com/pkg/errors"
	admission "k8s.io/api/admission/v1beta1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	meta_util "kmodules.xyz/client-go/meta"
	hookapi "kmodules.xyz/webhook-runtime/admission/v1beta1"
)

type MariaDBValidator struct {
	client      kubernetes.Interface
	extClient   cs.Interface
	lock        sync.RWMutex
	initialized bool
}

var _ hookapi.AdmissionHook = &MariaDBValidator{}

var forbiddenEnvVars = []string{
	"MARIADB_ROOT_PASSWORD",
	"MARIADB_ALLOW_EMPTY_PASSWORD",
	"MARIADB_RANDOM_ROOT_PASSWORD",
	"MARIADB_ONETIME_PASSWORD",
}

func (a *MariaDBValidator) Resource() (plural schema.GroupVersionResource, singular string) {
	return schema.GroupVersionResource{
			Group:    "validators.kubedb.com",
			Version:  "v1alpha1",
			Resource: "mariadbvalidators",
		},
		"mariadbvalidator"
}

func (a *MariaDBValidator) Initialize(config *rest.Config, stopCh <-chan struct{}) error {
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

func (a *MariaDBValidator) Admit(req *admission.AdmissionRequest) *admission.AdmissionResponse {
	status := &admission.AdmissionResponse{}

	if (req.Operation != admission.Create && req.Operation != admission.Update && req.Operation != admission.Delete) ||
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

	switch req.Operation {
	case admission.Delete:
		if req.Name != "" {
			// req.Object.Raw = nil, so read from kubernetes
			obj, err := a.extClient.KubedbV1alpha1().MariaDBs(req.Namespace).Get(req.Name, metav1.GetOptions{})
			if err != nil && !kerr.IsNotFound(err) {
				return hookapi.StatusInternalServerError(err)
			} else if err == nil && obj.Spec.TerminationPolicy == api.TerminationPolicyDoNotTerminate {
				return hookapi.StatusBadRequest(fmt.Errorf(`mariadb "%v/%v" can't be paused. To delete, change spec.terminationPolicy`, req.Namespace, req.Name))
			}
		}
	default:
		obj, err := meta_util.UnmarshalFromJSON(req.Object.Raw, api.SchemeGroupVersion)
		if err != nil {
			return hookapi.StatusBadRequest(err)
		}
		if req.Operation == admission.Update {
			// validate changes made by user
			oldObject, err := meta_util.UnmarshalFromJSON(req.OldObject.Raw, api.SchemeGroupVersion)
			if err != nil {
				return hookapi.StatusBadRequest(err)
			}

			mariadb := obj.(*api.MariaDB).DeepCopy()
			oldMariaDB := oldObject.(*api.MariaDB).DeepCopy()
			oldMariaDB.SetDefaults()
			// Allow changing Database Secret only if there was no secret have set up yet.
			if oldMariaDB.Spec.DatabaseSecret == nil {
				oldMariaDB.Spec.DatabaseSecret = mariadb.Spec.DatabaseSecret
			}

			if err := validateUpdate(mariadb, oldMariaDB, req.Kind.Kind); err != nil {
				return hookapi.StatusBadRequest(fmt.Errorf("%v", err))
			}
		}
		// validate database specs
		if err = ValidateMariaDB(a.client, a.extClient, obj.(*api.MariaDB), false); err != nil {
			return hookapi.StatusForbidden(err)
		}
	}
	status.Allowed = true
	return status
}

// recursivelyVersionCompare() receives two slices versionA and versionB of size 3 containing
// major, minor and patch parts of the given versions (versionA and versionB) in indices
// 0, 1 and 2 respectively. This function compares these parts of versionA and versionB. It returns,
//
// 		0;	if all parts of versionA are equal to corresponding parts of versionB
//		1;	if for some i, version[i] > versionB[i] where from j = 0 to i-1, versionA[j] = versionB[j]
//	   -1;	if for some i, version[i] < versionB[i] where from j = 0 to i-1, versionA[j] = versionB[j]
//
// ref: https://github.com/coreos/go-semver/blob/568e959cd89871e61434c1143528d9162da89ef2/semver/semver.go#L126-L141
func recursivelyVersionCompare(versionA []int64, versionB []int64) int {
	if len(versionA) == 0 {
		return 0
	}

	a := versionA[0]
	b := versionB[0]

	if a > b {
		return 1
	} else if a < b {
		return -1
	}

	return recursivelyVersionCompare(versionA[1:], versionB[1:])
}

// Currently, we support Group Replication for version 5.7.25. validateVersion()
// checks whether the given version has exactly these major (5), minor (7) and patch (25).
func validateGroupServerVersion(version string) error {
	recommended, err := semver.NewVersion(api.MariaDBGRRecommendedVersion)
	if err != nil {
		return fmt.Errorf("unable to parse recommended MariaDB version %s: %v", api.MariaDBGRRecommendedVersion, err)
	}

	given, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("unable to parse given MariaDB version %s: %v", version, err)
	}

	if cmp := recursivelyVersionCompare(recommended.Slice(), given.Slice()); cmp != 0 {
		return fmt.Errorf("currently supported MariaDB server version for group replication is %s, but used %s",
			api.MariaDBGRRecommendedVersion, version)
	}

	return nil
}

// On a replication master and each replication slave, the --server-id
// option must be specified to establish a unique replication ID in the
// range from 1 to 2^32 − 1. “Unique”, means that each ID must be different
// from every other ID in use by any other replication master or slave.
// ref: https://dev.mariadb.com/doc/refman/5.7/en/server-system-variables.html#sysvar_server_id
//
// We calculate a unique server-id for each server using baseServerID field in MariaDB CRD.
// Moreover we can use maximum of 9 servers in a group. So the baseServerID should be in
// range [0, (2^32 - 1) - 9]
func validateGroupBaseServerID(baseServerID uint) error {
	if uint(0) < baseServerID && baseServerID <= api.MariaDBMaxBaseServerID {
		return nil
	}
	return fmt.Errorf("invalid baseServerId specified, should be in range [1, %d]", api.MariaDBMaxBaseServerID)
}

func validateGroupReplicas(replicas int32) error {
	if replicas == 1 {
		return fmt.Errorf("group shouldn't start with 1 member, accepted value of 'spec.replicas' for group replication is in range [2, %d], default is %d if not specified",
			api.MariaDBMaxGroupMembers, api.MariaDBDefaultGroupSize)
	}

	if replicas > api.MariaDBMaxGroupMembers {
		return fmt.Errorf("group size can't be greater than max size %d (see https://dev.mariadb.com/doc/refman/5.7/en/group-replication-frequently-asked-questions.html",
			api.MariaDBMaxGroupMembers)
	}

	return nil
}

func validateMariaDBGroup(replicas int32, group api.MariaDBGroupSpec) error {
	if err := validateGroupReplicas(replicas); err != nil {
		return err
	}

	// validate group name whether it is a valid uuid
	if _, err := uuid.Parse(group.Name); err != nil {
		return errors.Wrapf(err, "invalid group name is set")
	}

	if err := validateGroupBaseServerID(*group.BaseServerID); err != nil {
		return err
	}

	return nil
}

// ValidateMariaDB checks if the object satisfies all the requirements.
// It is not method of Interface, because it is referenced from controller package too.
func ValidateMariaDB(client kubernetes.Interface, extClient cs.Interface, mariadb *api.MariaDB, strictValidation bool) error {
	var (
		err   error
		myVer *cat_api.MariaDBVersion
	)

	if mariadb.Spec.Version == "" {
		return errors.New(`'spec.version' is missing`)
	}
	if myVer, err = extClient.CatalogV1alpha1().MariaDBVersions().Get(string(mariadb.Spec.Version), metav1.GetOptions{}); err != nil {
		return err
	}

	if mariadb.Spec.Replicas == nil {
		return fmt.Errorf(`spec.replicas "%v" invalid. Value must be greater than 0, but for group replication this value shouldn't be more than %d'`,
			mariadb.Spec.Replicas, api.MariaDBMaxGroupMembers)
	}

	if mariadb.Spec.Topology != nil {
		if mariadb.Spec.Topology.Mode == nil {
			return errors.New("a valid 'spec.topology.mode' must be set for MariaDB clustering")
		}

		// currently supported cluster mode for MariaDB is "GroupReplication". So
		// '.spec.topology.mode' has been validated only for value "GroupReplication"
		if *mariadb.Spec.Topology.Mode != api.MariaDBClusterModeGroup {
			return errors.Errorf("currently supported cluster mode for MariaDB is %[1]q, spec.topology.mode must be %[1]q",
				api.MariaDBClusterModeGroup)
		}

		// validation for group configuration is performed only when
		// 'spec.topology.mode' is set to "GroupReplication"
		if *mariadb.Spec.Topology.Mode == api.MariaDBClusterModeGroup {
			// if spec.topology.mode is "GroupReplication", spec.topology.group is set to default during mutating
			if err = validateMariaDBGroup(*mariadb.Spec.Replicas, *mariadb.Spec.Topology.Group); err != nil {
				return err
			}
		}
	}

	if err := amv.ValidateEnvVar(mariadb.Spec.PodTemplate.Spec.Env, forbiddenEnvVars, api.ResourceKindMariaDB); err != nil {
		return err
	}

	if mariadb.Spec.StorageType == "" {
		return fmt.Errorf(`'spec.storageType' is missing`)
	}
	if mariadb.Spec.StorageType != api.StorageTypeDurable && mariadb.Spec.StorageType != api.StorageTypeEphemeral {
		return fmt.Errorf(`'spec.storageType' %s is invalid`, mariadb.Spec.StorageType)
	}
	if err := amv.ValidateStorage(client, mariadb.Spec.StorageType, mariadb.Spec.Storage); err != nil {
		return err
	}

	databaseSecret := mariadb.Spec.DatabaseSecret

	if strictValidation {
		if databaseSecret != nil {
			if _, err := client.CoreV1().Secrets(mariadb.Namespace).Get(databaseSecret.SecretName, metav1.GetOptions{}); err != nil {
				return err
			}
		}

		// Check if mariadbVersion is deprecated.
		// If deprecated, return error
		mariadbVersion, err := extClient.CatalogV1alpha1().MariaDBVersions().Get(string(mariadb.Spec.Version), metav1.GetOptions{})
		if err != nil {
			return err
		}

		if mariadbVersion.Spec.Deprecated {
			return fmt.Errorf("mariadb %s/%s is using deprecated version %v. Skipped processing", mariadb.Namespace, mariadb.Name, mariadbVersion.Name)
		}

		if mariadb.Spec.Topology != nil && mariadb.Spec.Topology.Mode != nil &&
			*mariadb.Spec.Topology.Mode == api.MariaDBClusterModeGroup {
			if err = validateGroupServerVersion(myVer.Spec.Version); err != nil {
				return err
			}
		}
	}

	if mariadb.Spec.Init != nil &&
		mariadb.Spec.Init.SnapshotSource != nil &&
		databaseSecret == nil {
		return fmt.Errorf("for Snapshot init, 'spec.databaseSecret.secretName' of %v/%v needs to be similar to older database of snapshot %v/%v",
			mariadb.Namespace, mariadb.Name, mariadb.Spec.Init.SnapshotSource.Namespace, mariadb.Spec.Init.SnapshotSource.Name)
	}

	backupScheduleSpec := mariadb.Spec.BackupSchedule
	if backupScheduleSpec != nil {
		if err := amv.ValidateBackupSchedule(client, backupScheduleSpec, mariadb.Namespace); err != nil {
			return err
		}
	}

	if mariadb.Spec.UpdateStrategy.Type == "" {
		return fmt.Errorf(`'spec.updateStrategy.type' is missing`)
	}

	if mariadb.Spec.TerminationPolicy == "" {
		return fmt.Errorf(`'spec.terminationPolicy' is missing`)
	}

	if mariadb.Spec.StorageType == api.StorageTypeEphemeral && mariadb.Spec.TerminationPolicy == api.TerminationPolicyPause {
		return fmt.Errorf(`'spec.terminationPolicy: Pause' can not be used for 'Ephemeral' storage`)
	}

	monitorSpec := mariadb.Spec.Monitor
	if monitorSpec != nil {
		if err := amv.ValidateMonitorSpec(monitorSpec); err != nil {
			return err
		}
	}

	if err := matchWithDormantDatabase(extClient, mariadb); err != nil {
		return err
	}
	return nil
}

func matchWithDormantDatabase(extClient cs.Interface, mariadb *api.MariaDB) error {
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
	drmnOriginSpec := dormantDb.Spec.Origin.Spec.MariaDB
	drmnOriginSpec.SetDefaults()
	originalSpec := mariadb.Spec

	// Skip checking UpdateStrategy
	drmnOriginSpec.UpdateStrategy = originalSpec.UpdateStrategy

	// Skip checking TerminationPolicy
	drmnOriginSpec.TerminationPolicy = originalSpec.TerminationPolicy

	// Skip checking ServiceAccountName
	drmnOriginSpec.PodTemplate.Spec.ServiceAccountName = originalSpec.PodTemplate.Spec.ServiceAccountName

	// Skip checking Monitoring
	drmnOriginSpec.Monitor = originalSpec.Monitor

	// Skip Checking Backup Scheduler
	drmnOriginSpec.BackupSchedule = originalSpec.BackupSchedule

	if !meta_util.Equal(drmnOriginSpec, &originalSpec) {
		diff := meta_util.Diff(drmnOriginSpec, &originalSpec)
		log.Errorf("mariadb spec mismatches with OriginSpec in DormantDatabases. Diff: %v", diff)
		return errors.New(fmt.Sprintf("mariadb spec mismatches with OriginSpec in DormantDatabases. Diff: %v", diff))
	}

	return nil
}

func validateUpdate(obj, oldObj runtime.Object, kind string) error {
	preconditions := getPreconditionFunc()
	_, err := meta_util.CreateStrategicPatch(oldObj, obj, preconditions...)
	if err != nil {
		if mergepatch.IsPreconditionFailed(err) {
			return fmt.Errorf("%v.%v", err, preconditionFailedError(kind))
		}
		return err
	}
	return nil
}

func getPreconditionFunc() []mergepatch.PreconditionFunc {
	preconditions := []mergepatch.PreconditionFunc{
		mergepatch.RequireKeyUnchanged("apiVersion"),
		mergepatch.RequireKeyUnchanged("kind"),
		mergepatch.RequireMetadataKeyUnchanged("name"),
		mergepatch.RequireMetadataKeyUnchanged("namespace"),
	}

	for _, field := range preconditionSpecFields {
		preconditions = append(preconditions,
			meta_util.RequireChainKeyUnchanged(field),
		)
	}
	return preconditions
}

var preconditionSpecFields = []string{
	"spec.storageType",
	"spec.storage",
	"spec.databaseSecret",
	"spec.init",
	"spec.podTemplate.spec.nodeSelector",
}

func preconditionFailedError(kind string) error {
	str := preconditionSpecFields
	strList := strings.Join(str, "\n\t")
	return fmt.Errorf(strings.Join([]string{`At least one of the following was changed:
	apiVersion
	kind
	name
	namespace`, strList}, "\n\t"))
}
