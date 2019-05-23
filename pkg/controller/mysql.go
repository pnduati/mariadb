package controller

import (
	"fmt"

	"github.com/appscode/go/encoding/json/types"
	"github.com/appscode/go/log"
	"github.com/kubedb/apimachinery/apis"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha1/util"
	"github.com/kubedb/apimachinery/pkg/eventer"
	validator "github.com/kubedb/mariadb/pkg/admission"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/reference"
	kutil "kmodules.xyz/client-go"
	dynamic_util "kmodules.xyz/client-go/dynamic"
	meta_util "kmodules.xyz/client-go/meta"
	storage "kmodules.xyz/objectstore-api/osm"
)

func (c *Controller) create(mariadb *api.MariaDB) error {
	if err := validator.ValidateMariaDB(c.Client, c.ExtClient, mariadb, true); err != nil {
		c.recorder.Event(
			mariadb,
			core.EventTypeWarning,
			eventer.EventReasonInvalid,
			err.Error(),
		)
		log.Errorln(err)
		// stop Scheduler in case there is any.
		c.cronController.StopBackupScheduling(mariadb.ObjectMeta)
		return nil
	}

	// Delete Matching DormantDatabase if exists any
	if err := c.deleteMatchingDormantDatabase(mariadb); err != nil {
		return fmt.Errorf(`failed to delete dormant Database : "%v/%v". Reason: %v`, mariadb.Namespace, mariadb.Name, err)
	}

	if mariadb.Status.Phase == "" {
		my, err := util.UpdateMariaDBStatus(c.ExtClient.KubedbV1alpha1(), mariadb, func(in *api.MariaDBStatus) *api.MariaDBStatus {
			in.Phase = api.DatabasePhaseCreating
			return in
		}, apis.EnableStatusSubresource)
		if err != nil {
			return err
		}
		mariadb.Status = my.Status
	}

	// create Governing Service
	governingService, err := c.createMariaDBGoverningService(mariadb)
	if err != nil {
		return fmt.Errorf(`failed to create Service: "%v/%v". Reason: %v`, mariadb.Namespace, governingService, err)
	}
	c.GoverningService = governingService

	if c.EnableRBAC {
		// Ensure ClusterRoles for statefulsets
		if err := c.ensureRBACStuff(mariadb); err != nil {
			return err
		}
	}

	// ensure database Service
	vt1, err := c.ensureService(mariadb)
	if err != nil {
		return err
	}

	if err := c.ensureDatabaseSecret(mariadb); err != nil {
		return err
	}

	// ensure database StatefulSet
	vt2, err := c.ensureStatefulSet(mariadb)
	if err != nil {
		return err
	}

	if vt1 == kutil.VerbCreated && vt2 == kutil.VerbCreated {
		c.recorder.Event(
			mariadb,
			core.EventTypeNormal,
			eventer.EventReasonSuccessful,
			"Successfully created MariaDB",
		)
	} else if vt1 == kutil.VerbPatched || vt2 == kutil.VerbPatched {
		c.recorder.Event(
			mariadb,
			core.EventTypeNormal,
			eventer.EventReasonSuccessful,
			"Successfully patched MariaDB",
		)
	}

	if _, err := meta_util.GetString(mariadb.Annotations, api.AnnotationInitialized); err == kutil.ErrNotFound &&
		mariadb.Spec.Init != nil && mariadb.Spec.Init.SnapshotSource != nil {

		snapshotSource := mariadb.Spec.Init.SnapshotSource

		if mariadb.Status.Phase == api.DatabasePhaseInitializing {
			return nil
		}
		jobName := fmt.Sprintf("%s-%s", api.DatabaseNamePrefix, snapshotSource.Name)
		if _, err := c.Client.BatchV1().Jobs(snapshotSource.Namespace).Get(jobName, metav1.GetOptions{}); err != nil {
			if !kerr.IsNotFound(err) {
				return err
			}
		} else {
			return nil
		}
		if err := c.initialize(mariadb); err != nil {
			return fmt.Errorf("failed to complete initialization for %v/%v. Reason: %v", mariadb.Namespace, mariadb.Name, err)
		}
		return nil
	}

	my, err := util.UpdateMariaDBStatus(c.ExtClient.KubedbV1alpha1(), mariadb, func(in *api.MariaDBStatus) *api.MariaDBStatus {
		in.Phase = api.DatabasePhaseRunning
		in.ObservedGeneration = types.NewIntHash(mariadb.Generation, meta_util.GenerationHash(mariadb))
		return in
	}, apis.EnableStatusSubresource)
	if err != nil {
		return err
	}
	mariadb.Status = my.Status

	// Ensure Schedule backup
	if err := c.ensureBackupScheduler(mariadb); err != nil {
		c.recorder.Eventf(
			mariadb,
			core.EventTypeWarning,
			eventer.EventReasonFailedToSchedule,
			err.Error(),
		)
		log.Errorln(err)
		// Don't return error. Continue processing rest.
	}

	// ensure StatsService for desired monitoring
	if _, err := c.ensureStatsService(mariadb); err != nil {
		c.recorder.Eventf(
			mariadb,
			core.EventTypeWarning,
			eventer.EventReasonFailedToCreate,
			"Failed to manage monitoring system. Reason: %v",
			err,
		)
		log.Errorln(err)
		return nil
	}

	if err := c.manageMonitor(mariadb); err != nil {
		c.recorder.Eventf(
			mariadb,
			core.EventTypeWarning,
			eventer.EventReasonFailedToCreate,
			"Failed to manage monitoring system. Reason: %v",
			err,
		)
		log.Errorln(err)
		return nil
	}

	_, err = c.ensureAppBinding(mariadb)
	if err != nil {
		log.Errorln(err)
		return err
	}

	return nil
}

func (c *Controller) ensureBackupScheduler(mariadb *api.MariaDB) error {
	mariadbVersion, err := c.ExtClient.CatalogV1alpha1().MariaDBVersions().Get(string(mariadb.Spec.Version), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get MariaDBVersion %v for %v/%v. Reason: %v", mariadb.Spec.Version, mariadb.Namespace, mariadb.Name, err)
	}
	// Setup Schedule backup
	if mariadb.Spec.BackupSchedule != nil {
		err := c.cronController.ScheduleBackup(mariadb, mariadb.Spec.BackupSchedule, mariadbVersion)
		if err != nil {
			return fmt.Errorf("failed to schedule snapshot for %v/%v. Reason: %v", mariadb.Namespace, mariadb.Name, err)
		}
	} else {
		c.cronController.StopBackupScheduling(mariadb.ObjectMeta)
	}
	return nil
}

func (c *Controller) initialize(mariadb *api.MariaDB) error {
	my, err := util.UpdateMariaDBStatus(c.ExtClient.KubedbV1alpha1(), mariadb, func(in *api.MariaDBStatus) *api.MariaDBStatus {
		in.Phase = api.DatabasePhaseInitializing
		return in
	}, apis.EnableStatusSubresource)
	if err != nil {
		return err
	}
	mariadb.Status = my.Status

	snapshotSource := mariadb.Spec.Init.SnapshotSource
	// Event for notification that kubernetes objects are creating
	c.recorder.Eventf(
		mariadb,
		core.EventTypeNormal,
		eventer.EventReasonInitializing,
		`Initializing from Snapshot: "%v"`,
		snapshotSource.Name,
	)

	namespace := snapshotSource.Namespace
	if namespace == "" {
		namespace = mariadb.Namespace
	}
	snapshot, err := c.ExtClient.KubedbV1alpha1().Snapshots(namespace).Get(snapshotSource.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	secret, err := storage.NewOSMSecret(c.Client, snapshot.OSMSecretName(), snapshot.Namespace, snapshot.Spec.Backend)
	if err != nil {
		return err
	}
	_, err = c.Client.CoreV1().Secrets(secret.Namespace).Create(secret)
	if err != nil && !kerr.IsAlreadyExists(err) {
		return err
	}

	job, err := c.createRestoreJob(mariadb, snapshot)
	if err != nil {
		return err
	}

	if err := c.SetJobOwnerReference(snapshot, job); err != nil {
		return err
	}

	return nil
}

func (c *Controller) terminate(mariadb *api.MariaDB) error {
	ref, rerr := reference.GetReference(clientsetscheme.Scheme, mariadb)
	if rerr != nil {
		return rerr
	}

	// If TerminationPolicy is "pause", keep everything (ie, PVCs,Secrets,Snapshots) intact.
	// In operator, create dormantdatabase
	if mariadb.Spec.TerminationPolicy == api.TerminationPolicyPause {
		if err := c.removeOwnerReferenceFromOffshoots(mariadb, ref); err != nil {
			return err
		}

		if _, err := c.createDormantDatabase(mariadb); err != nil {
			if kerr.IsAlreadyExists(err) {
				// if already exists, check if it is database of another Kind and return error in that case.
				// If the Kind is same, we can safely assume that the DormantDB was not deleted in before,
				// Probably because, User is more faster (create-delete-create-again-delete...) than operator!
				// So reuse that DormantDB!
				ddb, err := c.ExtClient.KubedbV1alpha1().DormantDatabases(mariadb.Namespace).Get(mariadb.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if val, _ := meta_util.GetStringValue(ddb.Labels, api.LabelDatabaseKind); val != api.ResourceKindMariaDB {
					return fmt.Errorf(`DormantDatabase "%v" of kind %v already exists`, mariadb.Name, val)
				}
			} else {
				return fmt.Errorf(`failed to create DormantDatabase: "%v/%v". Reason: %v`, mariadb.Namespace, mariadb.Name, err)
			}
		}
	} else {
		// If TerminationPolicy is "wipeOut", delete everything (ie, PVCs,Secrets,Snapshots).
		// If TerminationPolicy is "delete", delete PVCs and keep snapshots,secrets intact.
		// In both these cases, don't create dormantdatabase
		if err := c.setOwnerReferenceToOffshoots(mariadb, ref); err != nil {
			return err
		}
	}

	c.cronController.StopBackupScheduling(mariadb.ObjectMeta)

	if mariadb.Spec.Monitor != nil {
		if _, err := c.deleteMonitor(mariadb); err != nil {
			log.Errorln(err)
			return nil
		}
	}
	return nil
}

func (c *Controller) setOwnerReferenceToOffshoots(mariadb *api.MariaDB, ref *core.ObjectReference) error {
	selector := labels.SelectorFromSet(mariadb.OffshootSelectors())

	// If TerminationPolicy is "wipeOut", delete snapshots and secrets,
	// else, keep it intact.
	if mariadb.Spec.TerminationPolicy == api.TerminationPolicyWipeOut {
		if err := dynamic_util.EnsureOwnerReferenceForSelector(
			c.DynamicClient,
			api.SchemeGroupVersion.WithResource(api.ResourcePluralSnapshot),
			mariadb.Namespace,
			selector,
			ref); err != nil {
			return err
		}
		if err := c.wipeOutDatabase(mariadb.ObjectMeta, mariadb.Spec.GetSecrets(), ref); err != nil {
			return errors.Wrap(err, "error in wiping out database.")
		}
	} else {
		// Make sure snapshot and secret's ownerreference is removed.
		if err := dynamic_util.RemoveOwnerReferenceForSelector(
			c.DynamicClient,
			api.SchemeGroupVersion.WithResource(api.ResourcePluralSnapshot),
			mariadb.Namespace,
			selector,
			ref); err != nil {
			return err
		}
		if err := dynamic_util.RemoveOwnerReferenceForItems(
			c.DynamicClient,
			core.SchemeGroupVersion.WithResource("secrets"),
			mariadb.Namespace,
			mariadb.Spec.GetSecrets(),
			ref); err != nil {
			return err
		}
	}
	// delete PVC for both "wipeOut" and "delete" TerminationPolicy.
	return dynamic_util.EnsureOwnerReferenceForSelector(
		c.DynamicClient,
		core.SchemeGroupVersion.WithResource("persistentvolumeclaims"),
		mariadb.Namespace,
		selector,
		ref)
}

func (c *Controller) removeOwnerReferenceFromOffshoots(mariadb *api.MariaDB, ref *core.ObjectReference) error {
	// First, Get LabelSelector for Other Components
	labelSelector := labels.SelectorFromSet(mariadb.OffshootSelectors())

	if err := dynamic_util.RemoveOwnerReferenceForSelector(
		c.DynamicClient,
		api.SchemeGroupVersion.WithResource(api.ResourcePluralSnapshot),
		mariadb.Namespace,
		labelSelector,
		ref); err != nil {
		return err
	}
	if err := dynamic_util.RemoveOwnerReferenceForSelector(
		c.DynamicClient,
		core.SchemeGroupVersion.WithResource("persistentvolumeclaims"),
		mariadb.Namespace,
		labelSelector,
		ref); err != nil {
		return err
	}
	if err := dynamic_util.RemoveOwnerReferenceForItems(
		c.DynamicClient,
		core.SchemeGroupVersion.WithResource("secrets"),
		mariadb.Namespace,
		mariadb.Spec.GetSecrets(),
		ref); err != nil {
		return err
	}
	return nil
}
