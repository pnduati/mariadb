package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/appscode/go/log"
	"github.com/appscode/go/types"
	"github.com/fatih/structs"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/apimachinery/pkg/eventer"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/reference"
	kutil "kmodules.xyz/client-go"
	app_util "kmodules.xyz/client-go/apps/v1"
	core_util "kmodules.xyz/client-go/core/v1"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
)

func (c *Controller) ensureStatefulSet(mariadb *api.MariaDB) (kutil.VerbType, error) {
	if err := c.checkStatefulSet(mariadb); err != nil {
		return kutil.VerbUnchanged, err
	}

	// Create statefulSet for MariaDB database
	statefulSet, vt, err := c.createStatefulSet(mariadb)
	if err != nil {
		return kutil.VerbUnchanged, err
	}

	// Check StatefulSet Pod status
	if vt != kutil.VerbUnchanged {
		if err := c.checkStatefulSetPodStatus(statefulSet); err != nil {
			return kutil.VerbUnchanged, err
		}
		c.recorder.Eventf(
			mariadb,
			core.EventTypeNormal,
			eventer.EventReasonSuccessful,
			"Successfully %v StatefulSet",
			vt,
		)
	}
	return vt, nil
}

func (c *Controller) checkStatefulSet(mariadb *api.MariaDB) error {
	// SatatefulSet for MariaDB database
	statefulSet, err := c.Client.AppsV1().StatefulSets(mariadb.Namespace).Get(mariadb.OffshootName(), metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err) {
			return nil
		}
		return err
	}

	if statefulSet.Labels[api.LabelDatabaseKind] != api.ResourceKindMariaDB ||
		statefulSet.Labels[api.LabelDatabaseName] != mariadb.Name {
		return fmt.Errorf(`intended statefulSet "%v/%v" already exists`, mariadb.Namespace, mariadb.OffshootName())
	}

	return nil
}

func (c *Controller) createStatefulSet(mariadb *api.MariaDB) (*apps.StatefulSet, kutil.VerbType, error) {
	statefulSetMeta := metav1.ObjectMeta{
		Name:      mariadb.OffshootName(),
		Namespace: mariadb.Namespace,
	}

	ref, rerr := reference.GetReference(clientsetscheme.Scheme, mariadb)
	if rerr != nil {
		return nil, kutil.VerbUnchanged, rerr
	}

	mariadbVersion, err := c.ExtClient.CatalogV1alpha1().MariaDBVersions().Get(string(mariadb.Spec.Version), metav1.GetOptions{})
	if err != nil {
		return nil, kutil.VerbUnchanged, rerr
	}

	return app_util.CreateOrPatchStatefulSet(c.Client, statefulSetMeta, func(in *apps.StatefulSet) *apps.StatefulSet {
		in.Labels = mariadb.OffshootLabels()
		in.Annotations = mariadb.Spec.PodTemplate.Controller.Annotations
		core_util.EnsureOwnerReference(&in.ObjectMeta, ref)

		in.Spec.Replicas = mariadb.Spec.Replicas
		in.Spec.ServiceName = c.GoverningService
		in.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: mariadb.OffshootSelectors(),
		}
		in.Spec.Template.Labels = mariadb.OffshootSelectors()
		in.Spec.Template.Annotations = mariadb.Spec.PodTemplate.Annotations
		in.Spec.Template.Spec.InitContainers = core_util.UpsertContainers(
			in.Spec.Template.Spec.InitContainers,
			append(
				[]core.Container{
					{
						Name:            "remove-lost-found",
						Image:           mariadbVersion.Spec.InitContainer.Image,
						ImagePullPolicy: core.PullIfNotPresent,
						Command: []string{
							"rm",
							"-rf",
							"/var/lib/mariadb/lost+found",
						},
						VolumeMounts: []core.VolumeMount{
							{
								Name:      "data",
								MountPath: "/var/lib/mariadb",
							},
						},
						Resources: mariadb.Spec.PodTemplate.Spec.Resources,
					},
				},
				mariadb.Spec.PodTemplate.Spec.InitContainers...,
			),
		)

		container := core.Container{
			Name:            api.ResourceSingularMariaDB,
			Image:           mariadbVersion.Spec.DB.Image,
			ImagePullPolicy: core.PullIfNotPresent,
			Args:            mariadb.Spec.PodTemplate.Spec.Args,
			Resources:       mariadb.Spec.PodTemplate.Spec.Resources,
			LivenessProbe:   mariadb.Spec.PodTemplate.Spec.LivenessProbe,
			ReadinessProbe:  mariadb.Spec.PodTemplate.Spec.ReadinessProbe,
			Lifecycle:       mariadb.Spec.PodTemplate.Spec.Lifecycle,
			Ports: []core.ContainerPort{
				{
					Name:          "db",
					ContainerPort: api.MariaDBNodePort,
					Protocol:      core.ProtocolTCP,
				},
			},
		}
		if mariadb.Spec.Topology != nil && mariadb.Spec.Topology.Mode != nil &&
			*mariadb.Spec.Topology.Mode == api.MariaDBClusterModeGroup {
			container.Command = []string{
				"peer-finder",
			}
			userProvidedArgs := strings.Join(mariadb.Spec.PodTemplate.Spec.Args, " ")
			container.Args = []string{
				fmt.Sprintf("-service=%s", c.GoverningService),
				fmt.Sprintf("-on-start=/on-start.sh %s", userProvidedArgs),
			}
			if container.LivenessProbe != nil && structs.IsZero(*container.LivenessProbe) {
				container.LivenessProbe = nil
			}
			if container.ReadinessProbe != nil && structs.IsZero(*container.ReadinessProbe) {
				container.ReadinessProbe = nil
			}
		}
		in.Spec.Template.Spec.Containers = core_util.UpsertContainer(in.Spec.Template.Spec.Containers, container)

		if mariadb.GetMonitoringVendor() == mona.VendorPrometheus {
			in.Spec.Template.Spec.Containers = core_util.UpsertContainer(in.Spec.Template.Spec.Containers, core.Container{
				Name: "exporter",
				Command: []string{
					"/bin/sh",
				},
				Args: []string{
					"-c",
					// DATA_SOURCE_NAME=user:password@tcp(localhost:5555)/dbname
					// ref: https://github.com/prometheus/mariadbd_exporter#setting-the-mariadb-servers-data-source-name
					fmt.Sprintf(`export DATA_SOURCE_NAME="${MARIADB_ROOT_USERNAME:-}:${MARIADB_ROOT_PASSWORD:-}@(127.0.0.1:3306)/"
						/bin/mariadbd_exporter --web.listen-address=:%v --web.telemetry-path=%v %v`,
						mariadb.Spec.Monitor.Prometheus.Port, mariadb.StatsService().Path(), strings.Join(mariadb.Spec.Monitor.Args, " ")),
				},
				Image: mariadbVersion.Spec.Exporter.Image,
				Ports: []core.ContainerPort{
					{
						Name:          api.PrometheusExporterPortName,
						Protocol:      core.ProtocolTCP,
						ContainerPort: mariadb.Spec.Monitor.Prometheus.Port,
					},
				},
				Env:             mariadb.Spec.Monitor.Env,
				Resources:       mariadb.Spec.Monitor.Resources,
				SecurityContext: mariadb.Spec.Monitor.SecurityContext,
			})
		}
		// Set Admin Secret as MARIADB_ROOT_PASSWORD env variable
		in = upsertEnv(in, mariadb)
		in = upsertDataVolume(in, mariadb)
		in = upsertCustomConfig(in, mariadb)

		if mariadb.Spec.Init != nil && mariadb.Spec.Init.ScriptSource != nil {
			in = upsertInitScript(in, mariadb.Spec.Init.ScriptSource.VolumeSource)
		}

		in.Spec.Template.Spec.NodeSelector = mariadb.Spec.PodTemplate.Spec.NodeSelector
		in.Spec.Template.Spec.Affinity = mariadb.Spec.PodTemplate.Spec.Affinity
		if mariadb.Spec.PodTemplate.Spec.SchedulerName != "" {
			in.Spec.Template.Spec.SchedulerName = mariadb.Spec.PodTemplate.Spec.SchedulerName
		}
		in.Spec.Template.Spec.Tolerations = mariadb.Spec.PodTemplate.Spec.Tolerations
		in.Spec.Template.Spec.ImagePullSecrets = mariadb.Spec.PodTemplate.Spec.ImagePullSecrets
		in.Spec.Template.Spec.PriorityClassName = mariadb.Spec.PodTemplate.Spec.PriorityClassName
		in.Spec.Template.Spec.Priority = mariadb.Spec.PodTemplate.Spec.Priority
		in.Spec.Template.Spec.SecurityContext = mariadb.Spec.PodTemplate.Spec.SecurityContext

		if c.EnableRBAC {
			in.Spec.Template.Spec.ServiceAccountName = mariadb.OffshootName()
		}

		in.Spec.UpdateStrategy = mariadb.Spec.UpdateStrategy
		in = upsertUserEnv(in, mariadb)

		return in
	})
}

func upsertDataVolume(statefulSet *apps.StatefulSet, mariadb *api.MariaDB) *apps.StatefulSet {
	for i, container := range statefulSet.Spec.Template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB {
			volumeMount := core.VolumeMount{
				Name:      "data",
				MountPath: "/var/lib/mariadb",
			}
			volumeMounts := container.VolumeMounts
			volumeMounts = core_util.UpsertVolumeMount(volumeMounts, volumeMount)
			statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts = volumeMounts

			pvcSpec := mariadb.Spec.Storage
			if mariadb.Spec.StorageType == api.StorageTypeEphemeral {
				ed := core.EmptyDirVolumeSource{}
				if pvcSpec != nil {
					if sz, found := pvcSpec.Resources.Requests[core.ResourceStorage]; found {
						ed.SizeLimit = &sz
					}
				}
				statefulSet.Spec.Template.Spec.Volumes = core_util.UpsertVolume(
					statefulSet.Spec.Template.Spec.Volumes,
					core.Volume{
						Name: "data",
						VolumeSource: core.VolumeSource{
							EmptyDir: &ed,
						},
					})
			} else {
				if len(pvcSpec.AccessModes) == 0 {
					pvcSpec.AccessModes = []core.PersistentVolumeAccessMode{
						core.ReadWriteOnce,
					}
					log.Infof(`Using "%v" as AccessModes in mariadb.Spec.Storage`, core.ReadWriteOnce)
				}

				claim := core.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "data",
					},
					Spec: *pvcSpec,
				}
				if pvcSpec.StorageClassName != nil {
					claim.Annotations = map[string]string{
						"volume.beta.kubernetes.io/storage-class": *pvcSpec.StorageClassName,
					}
				}
				statefulSet.Spec.VolumeClaimTemplates = core_util.UpsertVolumeClaim(statefulSet.Spec.VolumeClaimTemplates, claim)
			}
			break
		}
	}
	return statefulSet
}

func upsertEnv(statefulSet *apps.StatefulSet, mariadb *api.MariaDB) *apps.StatefulSet {
	for i, container := range statefulSet.Spec.Template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB || container.Name == "exporter" {
			envs := []core.EnvVar{
				{
					Name: "MARIADB_ROOT_PASSWORD",
					ValueFrom: &core.EnvVarSource{
						SecretKeyRef: &core.SecretKeySelector{
							LocalObjectReference: core.LocalObjectReference{
								Name: mariadb.Spec.DatabaseSecret.SecretName,
							},
							Key: KeyMariaDBPassword,
						},
					},
				},
				{
					Name: "MARIADB_ROOT_USERNAME",
					ValueFrom: &core.EnvVarSource{
						SecretKeyRef: &core.SecretKeySelector{
							LocalObjectReference: core.LocalObjectReference{
								Name: mariadb.Spec.DatabaseSecret.SecretName,
							},
							Key: KeyMariaDBUser,
						},
					},
				},
			}
			if mariadb.Spec.Topology != nil &&
				mariadb.Spec.Topology.Mode != nil &&
				*mariadb.Spec.Topology.Mode == api.MariaDBClusterModeGroup &&
				container.Name == api.ResourceSingularMariaDB {
				envs = append(envs, []core.EnvVar{
					{
						Name:  "BASE_NAME",
						Value: mariadb.Name,
					},
					{
						Name:  "GOV_SVC",
						Value: mariadb.GoverningServiceName(),
					},
					{
						Name: "POD_NAMESPACE",
						ValueFrom: &core.EnvVarSource{
							FieldRef: &core.ObjectFieldSelector{
								FieldPath: "metadata.namespace",
							},
						},
					},
					{
						Name:  "GROUP_NAME",
						Value: mariadb.Spec.Topology.Group.Name,
					},
					{
						Name:  "BASE_SERVER_ID",
						Value: strconv.Itoa(int(*mariadb.Spec.Topology.Group.BaseServerID)),
					},
				}...)
			}
			statefulSet.Spec.Template.Spec.Containers[i].Env = core_util.UpsertEnvVars(container.Env, envs...)
		}
	}

	return statefulSet
}

// upsertUserEnv add/overwrite env from user provided env in crd spec
func upsertUserEnv(statefulSet *apps.StatefulSet, mariadb *api.MariaDB) *apps.StatefulSet {
	for i, container := range statefulSet.Spec.Template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB {
			statefulSet.Spec.Template.Spec.Containers[i].Env = core_util.UpsertEnvVars(container.Env, mariadb.Spec.PodTemplate.Spec.Env...)
			return statefulSet
		}
	}
	return statefulSet
}

func upsertInitScript(statefulSet *apps.StatefulSet, script core.VolumeSource) *apps.StatefulSet {
	for i, container := range statefulSet.Spec.Template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB {
			volumeMount := core.VolumeMount{
				Name:      "initial-script",
				MountPath: "/docker-entrypoint-initdb.d",
			}
			statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts = core_util.UpsertVolumeMount(
				container.VolumeMounts,
				volumeMount,
			)

			volume := core.Volume{
				Name:         "initial-script",
				VolumeSource: script,
			}
			statefulSet.Spec.Template.Spec.Volumes = core_util.UpsertVolume(
				statefulSet.Spec.Template.Spec.Volumes,
				volume,
			)
			return statefulSet
		}
	}
	return statefulSet
}

func (c *Controller) checkStatefulSetPodStatus(statefulSet *apps.StatefulSet) error {
	err := core_util.WaitUntilPodRunningBySelector(
		c.Client,
		statefulSet.Namespace,
		statefulSet.Spec.Selector,
		int(types.Int32(statefulSet.Spec.Replicas)),
	)
	if err != nil {
		return err
	}
	return nil
}

func upsertCustomConfig(statefulSet *apps.StatefulSet, mariadb *api.MariaDB) *apps.StatefulSet {
	if mariadb.Spec.ConfigSource != nil {
		for i, container := range statefulSet.Spec.Template.Spec.Containers {
			if container.Name == api.ResourceSingularMariaDB {
				configVolumeMount := core.VolumeMount{
					Name:      "custom-config",
					MountPath: "/etc/mariadb/conf.d",
				}
				volumeMounts := container.VolumeMounts
				volumeMounts = core_util.UpsertVolumeMount(volumeMounts, configVolumeMount)
				statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts = volumeMounts

				configVolume := core.Volume{
					Name:         "custom-config",
					VolumeSource: *mariadb.Spec.ConfigSource,
				}

				volumes := statefulSet.Spec.Template.Spec.Volumes
				volumes = core_util.UpsertVolume(volumes, configVolume)
				statefulSet.Spec.Template.Spec.Volumes = volumes
				break
			}
		}
	}
	return statefulSet
}
