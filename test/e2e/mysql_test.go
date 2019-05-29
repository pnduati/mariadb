package e2e_test

import (
	"fmt"
	"os"

	"github.com/appscode/go/log"
	"github.com/appscode/go/types"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha1/util"
	"github.com/kubedb/mariadb/test/e2e/framework"
	"github.com/kubedb/mariadb/test/e2e/matcher"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_util "kmodules.xyz/client-go/meta"
	store "kmodules.xyz/objectstore-api/api/v1"
	stashV1alpha1 "stash.appscode.dev/stash/apis/stash/v1alpha1"
	stashV1beta1 "stash.appscode.dev/stash/apis/stash/v1beta1"
)

const (
	S3_BUCKET_NAME        = "S3_BUCKET_NAME"
	GCS_BUCKET_NAME       = "GCS_BUCKET_NAME"
	AZURE_CONTAINER_NAME  = "AZURE_CONTAINER_NAME"
	SWIFT_CONTAINER_NAME  = "SWIFT_CONTAINER_NAME"
	MARIADB_DATABASE      = "MARIADB_DATABASE"
	MARIADB_ROOT_PASSWORD = "MARIADB_ROOT_PASSWORD"
)

var _ = Describe("MariaDB", func() {
	var (
		err              error
		f                *framework.Invocation
		mariadb          *api.MariaDB
		garbageMariaDB   *api.MariaDBList
		snapshot         *api.Snapshot
		secret           *core.Secret
		skipMessage      string
		skipDataChecking bool
		dbName           string
	)

	BeforeEach(func() {
		f = root.Invoke()
		mariadb = f.MariaDB()
		garbageMariaDB = new(api.MariaDBList)
		snapshot = f.Snapshot()
		skipMessage = ""
		skipDataChecking = true
		dbName = "mariadb"
	})

	var createAndWaitForRunning = func() {
		By("Create MariaDB: " + mariadb.Name)
		err = f.CreateMariaDB(mariadb)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for Running mariadb")
		f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

		By("Wait for AppBinding to create")
		f.EventuallyAppBinding(mariadb.ObjectMeta).Should(BeTrue())

		By("Check valid AppBinding Specs")
		err := f.CheckAppBindingSpec(mariadb.ObjectMeta)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for database to be ready")
		f.EventuallyDatabaseReady(mariadb.ObjectMeta, dbName).Should(BeTrue())
	}

	var testGeneralBehaviour = func() {
		if skipMessage != "" {
			Skip(skipMessage)
		}
		// Create MariaDB
		createAndWaitForRunning()

		By("Creating Table")
		f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

		By("Inserting Rows")
		f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

		By("Checking Row Count of Table")
		f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

		By("Delete mariadb")
		err = f.DeleteMariaDB(mariadb.ObjectMeta)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for mariadb to be paused")
		f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

		// Create MariaDB object again to resume it
		By("Create MariaDB: " + mariadb.Name)
		err = f.CreateMariaDB(mariadb)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for DormantDatabase to be deleted")
		f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

		By("Wait for Running mariadb")
		f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

		By("Checking Row Count of Table")
		f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

	}

	var shouldTakeSnapshot = func() {
		// Create and wait for running MariaDB
		createAndWaitForRunning()

		By("Create Secret")
		err := f.CreateSecret(secret)
		Expect(err).NotTo(HaveOccurred())

		By("Create Snapshot")
		err = f.CreateSnapshot(snapshot)
		Expect(err).NotTo(HaveOccurred())

		By("Check for Succeed snapshot")
		f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseSucceeded))

		if !skipDataChecking {
			By("Check for snapshot data")
			f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
		}
	}

	var shouldInsertDataAndTakeSnapshot = func() {
		// Create and wait for running MariaDB
		createAndWaitForRunning()

		By("Creating Table")
		f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

		By("Inserting Row")
		f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

		By("Checking Row Count of Table")
		f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

		By("Create Secret")
		err := f.CreateSecret(secret)
		Expect(err).NotTo(HaveOccurred())

		By("Create Snapshot")
		err = f.CreateSnapshot(snapshot)
		Expect(err).NotTo(HaveOccurred())

		By("Check for Succeed snapshot")
		f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseSucceeded))

		if !skipDataChecking {
			By("Check for snapshot data")
			f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
		}
	}

	var deleteTestResource = func() {
		if mariadb == nil {
			log.Infoln("Skipping cleanup. Reason: mariadb is nil")
			return
		}

		By("Check if mariadb " + mariadb.Name + " exists.")
		my, err := f.GetMariaDB(mariadb.ObjectMeta)
		if err != nil {
			if kerr.IsNotFound(err) {
				// MariaDB was not created. Hence, rest of cleanup is not necessary.
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}

		By("Delete mariadb")
		err = f.DeleteMariaDB(mariadb.ObjectMeta)
		if err != nil {
			if kerr.IsNotFound(err) {
				log.Infoln("Skipping rest of the cleanup. Reason: MariaDB does not exist.")
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}

		if my.Spec.TerminationPolicy == api.TerminationPolicyPause {
			By("Wait for mariadb to be paused")
			f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

			By("WipeOut mariadb")
			_, err := f.PatchDormantDatabase(mariadb.ObjectMeta, func(in *api.DormantDatabase) *api.DormantDatabase {
				in.Spec.WipeOut = true
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Delete Dormant Database")
			err = f.DeleteDormantDatabase(mariadb.ObjectMeta)
			Expect(err).NotTo(HaveOccurred())
		}

		By("Wait for mariadb resources to be wipedOut")
		f.EventuallyWipedOut(mariadb.ObjectMeta).Should(Succeed())
	}

	var deleteSnapshot = func() {

		By("Deleting Snapshot: " + snapshot.Name)
		err = f.DeleteSnapshot(snapshot.ObjectMeta)
		if err != nil && !kerr.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		if !skipDataChecking {
			// do not try to check snapshot data if secret does not exist
			_, err = f.GetSecret(secret.ObjectMeta)
			if err != nil && kerr.IsNotFound(err) {
				log.Infof("Skipping checking snapshot data. Reason: secret %s not found", secret.Name)
				return
			}
			Expect(err).NotTo(HaveOccurred())

			By("Checking Snapshot's data wiped out from backend")
			f.EventuallySnapshotDataFound(snapshot).Should(BeFalse())
		}
	}

	AfterEach(func() {
		// delete resources for current MariaDB
		deleteTestResource()

		// old MariaDB are in garbageMariaDB list. delete their resources.
		for _, my := range garbageMariaDB.Items {
			*mariadb = my
			deleteTestResource()
		}

		By("Delete left over workloads if exists any")
		f.CleanWorkloadLeftOvers()
	})

	Describe("Test", func() {

		Context("General", func() {

			Context("-", func() {
				It("should run successfully", testGeneralBehaviour)
			})

			Context("with custom SA Name", func() {
				BeforeEach(func() {
					var customSecret *core.Secret
					customSecret = f.SecretForDatabaseAuthentication(mariadb.ObjectMeta, false)
					mariadb.Spec.DatabaseSecret = &core.SecretVolumeSource{
						SecretName: customSecret.Name,
					}
					err := f.CreateSecret(customSecret)
					Expect(err).NotTo(HaveOccurred())
					mariadb.Spec.PodTemplate.Spec.ServiceAccountName = "my-custom-sa"
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyPause
				})

				It("should start and resume successfully", func() {
					//shouldTakeSnapshot()
					createAndWaitForRunning()
					if mariadb == nil {
						Skip("Skipping")
					}
					By("Check if Postgres " + mariadb.Name + " exists.")
					_, err := f.GetMariaDB(mariadb.ObjectMeta)
					if err != nil {
						if kerr.IsNotFound(err) {
							// Postgres was not created. Hence, rest of cleanup is not necessary.
							return
						}
						Expect(err).NotTo(HaveOccurred())
					}

					By("Delete mariadb: " + mariadb.Name)
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					if err != nil {
						if kerr.IsNotFound(err) {
							// Postgres was not created. Hence, rest of cleanup is not necessary.
							log.Infof("Skipping rest of cleanup. Reason: Postgres %s is not found.", mariadb.Name)
							return
						}
						Expect(err).NotTo(HaveOccurred())
					}

					By("Wait for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					By("Resume DB")
					createAndWaitForRunning()
				})
			})

			Context("PDB", func() {

				It("should run eviction successfully", func() {
					mariadb.Spec.Replicas = types.Int32P(3)
					// Create MariaDB
					By("Create and run MariaDB with three replicas")
					createAndWaitForRunning()
					//Evict MariaDB pods
					By("Try to evict pods")
					err := f.EvictPodsFromStatefulSet(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("Snapshot", func() {

			BeforeEach(func() {
				skipDataChecking = false
				snapshot.Spec.DatabaseName = mariadb.Name
			})

			AfterEach(func() {
				// delete snapshot and check for data wipeOut
				deleteSnapshot()

				By("Deleting secret: " + secret.Name)
				err := f.DeleteSecret(secret.ObjectMeta)
				if err != nil && !kerr.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred())
				}
			})

			Context("For Custom Resources", func() {

				BeforeEach(func() {
					snapshot.Spec.DatabaseName = mariadb.Name
					secret = f.SecretForGCSBackend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.GCS = &store.GCSSpec{
						Bucket: os.Getenv(GCS_BUCKET_NAME),
					}
				})

				Context("with custom SA", func() {
					var customSAForDB *core.ServiceAccount
					var customRoleForDB *rbac.Role
					var customRoleBindingForDB *rbac.RoleBinding
					var customSAForSnapshot *core.ServiceAccount
					var customRoleForSnapshot *rbac.Role
					var customRoleBindingForSnapshot *rbac.RoleBinding
					BeforeEach(func() {
						mariadb.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
						customSAForDB = f.ServiceAccount()
						mariadb.Spec.PodTemplate.Spec.ServiceAccountName = customSAForDB.Name
						customRoleForDB = f.RoleForMariaDB(mariadb.ObjectMeta)
						customRoleBindingForDB = f.RoleBinding(customSAForDB.Name, customRoleForDB.Name)

						customSAForSnapshot = f.ServiceAccount()
						snapshot.Spec.PodTemplate.Spec.ServiceAccountName = customSAForSnapshot.Name
						customRoleForSnapshot = f.RoleForSnapshot(mariadb.ObjectMeta)
						customRoleBindingForSnapshot = f.RoleBinding(customSAForSnapshot.Name, customRoleForSnapshot.Name)

						By("Create Database SA")
						err = f.CreateServiceAccount(customSAForDB)
						Expect(err).NotTo(HaveOccurred())
						By("Create Database Role")
						err = f.CreateRole(customRoleForDB)
						Expect(err).NotTo(HaveOccurred())
						By("Create Database RoleBinding")
						err = f.CreateRoleBinding(customRoleBindingForDB)
						Expect(err).NotTo(HaveOccurred())

						By("Create Snapshot SA")
						err = f.CreateServiceAccount(customSAForSnapshot)
						Expect(err).NotTo(HaveOccurred())
						By("Create Snapshot Role")
						err = f.CreateRole(customRoleForSnapshot)
						Expect(err).NotTo(HaveOccurred())
						By("Create Snapshot RoleBinding")
						err = f.CreateRoleBinding(customRoleBindingForSnapshot)
						Expect(err).NotTo(HaveOccurred())
					})

					It("should take snapshot successfully", func() {
						shouldInsertDataAndTakeSnapshot()
					})

					It("should initialize from snapshot successfully", func() {
						// Create MariaDB and take Snapshot
						shouldInsertDataAndTakeSnapshot()

						oldMariaDB, err := f.GetMariaDB(mariadb.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())
						garbageMariaDB.Items = append(garbageMariaDB.Items, *oldMariaDB)

						By("Create mariadb from snapshot")
						mariadb = f.MariaDB()
						mariadb.Spec.Init = &api.InitSpec{
							SnapshotSource: &api.SnapshotSourceSpec{
								Namespace: snapshot.Namespace,
								Name:      snapshot.Name,
							},
						}

						By("Creating init Snapshot Mysql without secret name" + mariadb.Name)
						err = f.CreateMariaDB(mariadb)
						Expect(err).Should(HaveOccurred())

						// for snapshot init, user have to use older secret,
						// because the username & password  will be replaced to
						mariadb.Spec.DatabaseSecret = oldMariaDB.Spec.DatabaseSecret

						//Create New role and bind it to existing SA
						By("Get new Role and RB")
						customRoleForReplayDB := f.RoleForMariaDB(mariadb.ObjectMeta)
						customRoleBindingForReplayDB := f.RoleBinding(customSAForDB.Name, customRoleForReplayDB.Name)

						By("Create Database Role")
						err = f.CreateRole(customRoleForReplayDB)
						Expect(err).NotTo(HaveOccurred())
						By("Create Database RoleBinding")
						err = f.CreateRoleBinding(customRoleBindingForReplayDB)
						Expect(err).NotTo(HaveOccurred())

						mariadb.Spec.PodTemplate.Spec.ServiceAccountName = customSAForDB.Name

						// Create and wait for running MariaDB
						By("Create DB with snapshot data")
						createAndWaitForRunning()

						By("Checking Row Count of Table")
						f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
					})
				})

				Context("with custom Secret", func() {
					var customSecret *core.Secret
					BeforeEach(func() {
						customSecret = f.SecretForDatabaseAuthentication(mariadb.ObjectMeta, false)
						mariadb.Spec.DatabaseSecret = &core.SecretVolumeSource{
							SecretName: customSecret.Name,
						}

					})

					It("should not delete database secret", func() {
						By("Create Database Secret")
						err := f.CreateSecret(customSecret)
						Expect(err).NotTo(HaveOccurred())

						By("Take Snapshot")
						shouldTakeSnapshot()
						By("Delete test resources")
						deleteTestResource()
						By("Confirm Database Secret exists")
						err = f.CheckSecret(customSecret)
						Expect(err).NotTo(HaveOccurred())
					})

					It("should delete database secret", func() {
						kubedbSecret := f.SecretForDatabaseAuthentication(mariadb.ObjectMeta, true)
						mariadb.Spec.DatabaseSecret = &core.SecretVolumeSource{
							SecretName: kubedbSecret.Name,
						}
						By("Create Database Secret")
						err = f.CreateSecret(kubedbSecret)
						Expect(err).NotTo(HaveOccurred())

						By("Take Snapshot")
						shouldTakeSnapshot()
						By("Delete test resources")
						deleteTestResource()
						By("Confirm Database Secret doesnt exist")
						err = f.CheckSecret(kubedbSecret)
						Expect(err).To(HaveOccurred())
					})
				})
			})

			Context("In Local", func() {

				BeforeEach(func() {
					skipDataChecking = true
					secret = f.SecretForLocalBackend()
					snapshot.Spec.StorageSecretName = secret.Name
				})

				Context("With EmptyDir as Snapshot's backend", func() {
					BeforeEach(func() {
						snapshot.Spec.Local = &store.LocalSpec{
							MountPath: "/repo",
							VolumeSource: core.VolumeSource{
								EmptyDir: &core.EmptyDirVolumeSource{},
							},
						}
					})

					It("should take Snapshot successfully", shouldTakeSnapshot)
				})

				Context("With PVC as Snapshot's backend", func() {
					var snapPVC *core.PersistentVolumeClaim

					BeforeEach(func() {
						snapPVC = f.GetPersistentVolumeClaim()
						err := f.CreatePersistentVolumeClaim(snapPVC)
						Expect(err).NotTo(HaveOccurred())

						snapshot.Spec.Local = &store.LocalSpec{
							MountPath: "/repo",
							VolumeSource: core.VolumeSource{
								PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
									ClaimName: snapPVC.Name,
								},
							},
						}
					})

					AfterEach(func() {
						err := f.DeletePersistentVolumeClaim(snapPVC.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())
					})

					It("should delete Snapshot successfully", func() {
						shouldTakeSnapshot()

						By("Deleting Snapshot")
						err := f.DeleteSnapshot(snapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Waiting for Snapshot to be deleted")
						f.EventuallySnapshot(snapshot.ObjectMeta).Should(BeFalse())
					})
				})
			})

			Context("In S3", func() {
				BeforeEach(func() {
					secret = f.SecretForS3Backend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.S3 = &store.S3Spec{
						Bucket: os.Getenv(S3_BUCKET_NAME),
					}
				})

				It("should take Snapshot successfully", shouldInsertDataAndTakeSnapshot)

				Context("faulty snapshot", func() {
					BeforeEach(func() {
						skipDataChecking = true
						snapshot.Spec.StorageSecretName = secret.Name
						snapshot.Spec.S3 = &store.S3Spec{
							Bucket: "nonexisting",
						}
					})

					It("snapshot should fail", func() {
						// Create and wait for running db
						createAndWaitForRunning()

						By("Create Secret")
						err := f.CreateSecret(secret)
						Expect(err).NotTo(HaveOccurred())

						By("Create Snapshot")
						err = f.CreateSnapshot(snapshot)
						Expect(err).NotTo(HaveOccurred())

						By("Check for failed snapshot")
						f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseFailed))
					})
				})

				Context("Delete One Snapshot keeping others", func() {
					BeforeEach(func() {
						mariadb.Spec.Init = &api.InitSpec{
							ScriptSource: &api.ScriptSourceSpec{
								VolumeSource: core.VolumeSource{
									GitRepo: &core.GitRepoVolumeSource{
										Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
										Directory:  ".",
									},
								},
							},
						}
					})

					It("Delete One Snapshot keeping others", func() {
						// Create MariaDB and take Snapshot
						shouldTakeSnapshot()

						oldSnapshot := snapshot.DeepCopy()

						// New snapshot that has old snapshot's name in prefix
						snapshot.Name += "-2"

						By(fmt.Sprintf("Create Snapshot %v", snapshot.Name))
						err = f.CreateSnapshot(snapshot)
						Expect(err).NotTo(HaveOccurred())

						By("Check for Succeeded snapshot")
						f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseSucceeded))

						if !skipDataChecking {
							By("Check for snapshot data")
							f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
						}

						// delete old snapshot
						By(fmt.Sprintf("Delete old Snapshot %v", oldSnapshot.Name))
						err = f.DeleteSnapshot(oldSnapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Waiting for old Snapshot to be deleted")
						f.EventuallySnapshot(oldSnapshot.ObjectMeta).Should(BeFalse())
						if !skipDataChecking {
							By(fmt.Sprintf("Check data for old snapshot %v", oldSnapshot.Name))
							f.EventuallySnapshotDataFound(oldSnapshot).Should(BeFalse())
						}

						// check remaining snapshot
						By(fmt.Sprintf("Checking another Snapshot %v still exists", snapshot.Name))
						_, err = f.GetSnapshot(snapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						if !skipDataChecking {
							By(fmt.Sprintf("Check data for remaining snapshot %v", snapshot.Name))
							f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
						}
					})
				})
			})

			Context("In GCS", func() {
				BeforeEach(func() {
					secret = f.SecretForGCSBackend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.GCS = &store.GCSSpec{
						Bucket: os.Getenv(GCS_BUCKET_NAME),
					}
				})

				Context("Without Init", func() {
					It("should take Snapshot successfully", shouldInsertDataAndTakeSnapshot)
				})

				Context("With Init", func() {
					BeforeEach(func() {
						mariadb.Spec.Init = &api.InitSpec{
							ScriptSource: &api.ScriptSourceSpec{
								VolumeSource: core.VolumeSource{
									GitRepo: &core.GitRepoVolumeSource{
										Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
										Directory:  ".",
									},
								},
							},
						}
					})

					It("should take Snapshot successfully", shouldTakeSnapshot)
				})

				Context("Delete One Snapshot keeping others", func() {
					BeforeEach(func() {
						mariadb.Spec.Init = &api.InitSpec{
							ScriptSource: &api.ScriptSourceSpec{
								VolumeSource: core.VolumeSource{
									GitRepo: &core.GitRepoVolumeSource{
										Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
										Directory:  ".",
									},
								},
							},
						}
					})

					It("Delete One Snapshot keeping others", func() {
						// Create MariaDB and take Snapshot
						shouldTakeSnapshot()

						oldSnapshot := snapshot.DeepCopy()

						// New snapshot that has old snapshot's name in prefix
						snapshot.Name += "-2"

						By(fmt.Sprintf("Create Snapshot %v", snapshot.Name))
						err = f.CreateSnapshot(snapshot)
						Expect(err).NotTo(HaveOccurred())

						By("Check for Succeeded snapshot")
						f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseSucceeded))

						if !skipDataChecking {
							By("Check for snapshot data")
							f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
						}

						// delete old snapshot
						By(fmt.Sprintf("Delete old Snapshot %v", oldSnapshot.Name))
						err = f.DeleteSnapshot(oldSnapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Waiting for old Snapshot to be deleted")
						f.EventuallySnapshot(oldSnapshot.ObjectMeta).Should(BeFalse())
						if !skipDataChecking {
							By(fmt.Sprintf("Check data for old snapshot %v", oldSnapshot.Name))
							f.EventuallySnapshotDataFound(oldSnapshot).Should(BeFalse())
						}

						// check remaining snapshot
						By(fmt.Sprintf("Checking another Snapshot %v still exists", snapshot.Name))
						_, err = f.GetSnapshot(snapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						if !skipDataChecking {
							By(fmt.Sprintf("Check data for remaining snapshot %v", snapshot.Name))
							f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
						}
					})
				})

			})

			Context("In Azure", func() {
				BeforeEach(func() {
					secret = f.SecretForAzureBackend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.Azure = &store.AzureSpec{
						Container: os.Getenv(AZURE_CONTAINER_NAME),
					}
				})

				It("should take Snapshot successfully", shouldInsertDataAndTakeSnapshot)

				Context("Delete One Snapshot keeping others", func() {
					BeforeEach(func() {
						mariadb.Spec.Init = &api.InitSpec{
							ScriptSource: &api.ScriptSourceSpec{
								VolumeSource: core.VolumeSource{
									GitRepo: &core.GitRepoVolumeSource{
										Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
										Directory:  ".",
									},
								},
							},
						}
					})

					It("Delete One Snapshot keeping others", func() {
						// Create MariaDB and take Snapshot
						shouldTakeSnapshot()

						oldSnapshot := snapshot.DeepCopy()

						// New snapshot that has old snapshot's name in prefix
						snapshot.Name += "-2"

						By(fmt.Sprintf("Create Snapshot %v", snapshot.Name))
						err = f.CreateSnapshot(snapshot)
						Expect(err).NotTo(HaveOccurred())

						By("Check for Succeeded snapshot")
						f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseSucceeded))

						if !skipDataChecking {
							By("Check for snapshot data")
							f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
						}

						// delete old snapshot
						By(fmt.Sprintf("Delete old Snapshot %v", oldSnapshot.Name))
						err = f.DeleteSnapshot(oldSnapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Waiting for old Snapshot to be deleted")
						f.EventuallySnapshot(oldSnapshot.ObjectMeta).Should(BeFalse())
						if !skipDataChecking {
							By(fmt.Sprintf("Check data for old snapshot %v", oldSnapshot.Name))
							f.EventuallySnapshotDataFound(oldSnapshot).Should(BeFalse())
						}

						// check remaining snapshot
						By(fmt.Sprintf("Checking another Snapshot %v still exists", snapshot.Name))
						_, err = f.GetSnapshot(snapshot.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						if !skipDataChecking {
							By(fmt.Sprintf("Check data for remaining snapshot %v", snapshot.Name))
							f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
						}
					})
				})
			})

			Context("In Swift", func() {
				BeforeEach(func() {
					secret = f.SecretForSwiftBackend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.Swift = &store.SwiftSpec{
						Container: os.Getenv(SWIFT_CONTAINER_NAME),
					}
				})

				It("should take Snapshot successfully", shouldInsertDataAndTakeSnapshot)
			})

			Context("Snapshot PodVolume Template - In S3", func() {

				BeforeEach(func() {
					secret = f.SecretForS3Backend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.S3 = &store.S3Spec{
						Bucket: os.Getenv(S3_BUCKET_NAME),
					}
				})

				var shouldHandleJobVolumeSuccessfully = func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Get MariaDB")
					es, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
					mariadb.Spec = es.Spec

					By("Create Secret")
					err = f.CreateSecret(secret)
					Expect(err).NotTo(HaveOccurred())

					// determine pvcSpec and storageType for job
					// start
					pvcSpec := snapshot.Spec.PodVolumeClaimSpec
					if pvcSpec == nil {
						pvcSpec = mariadb.Spec.Storage
					}
					st := snapshot.Spec.StorageType
					if st == nil {
						st = &mariadb.Spec.StorageType
					}
					Expect(st).NotTo(BeNil())
					// end

					By("Create Snapshot")
					err = f.CreateSnapshot(snapshot)
					if *st == api.StorageTypeDurable && pvcSpec == nil {
						By("Create Snapshot should have failed")
						Expect(err).Should(HaveOccurred())
						return
					} else {
						Expect(err).NotTo(HaveOccurred())
					}

					By("Get Snapshot")
					snap, err := f.GetSnapshot(snapshot.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
					snapshot.Spec = snap.Spec

					if *st == api.StorageTypeEphemeral {
						storageSize := "0"
						if pvcSpec != nil {
							if sz, found := pvcSpec.Resources.Requests[core.ResourceStorage]; found {
								storageSize = sz.String()
							}
						}
						By(fmt.Sprintf("Check for Job Empty volume size: %v", storageSize))
						f.EventuallyJobVolumeEmptyDirSize(snapshot.ObjectMeta).Should(Equal(storageSize))
					} else if *st == api.StorageTypeDurable {
						sz, found := pvcSpec.Resources.Requests[core.ResourceStorage]
						Expect(found).NotTo(BeFalse())

						By("Check for Job PVC Volume size from snapshot")
						f.EventuallyJobPVCSize(snapshot.ObjectMeta).Should(Equal(sz.String()))
					}

					By("Check for succeeded snapshot")
					f.EventuallySnapshotPhase(snapshot.ObjectMeta).Should(Equal(api.SnapshotPhaseSucceeded))

					if !skipDataChecking {
						By("Check for snapshot data")
						f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
					}
				}

				// db StorageType Scenarios
				// ==============> Start
				var dbStorageTypeScenarios = func() {
					Context("DBStorageType - Durable", func() {
						BeforeEach(func() {
							mariadb.Spec.StorageType = api.StorageTypeDurable
							mariadb.Spec.Storage = &core.PersistentVolumeClaimSpec{
								Resources: core.ResourceRequirements{
									Requests: core.ResourceList{
										core.ResourceStorage: resource.MustParse(framework.DBPvcStorageSize),
									},
								},
								StorageClassName: types.StringP(root.StorageClass),
							}

						})

						It("should Handle Job Volume Successfully", shouldHandleJobVolumeSuccessfully)
					})

					Context("DBStorageType - Ephemeral", func() {
						BeforeEach(func() {
							mariadb.Spec.StorageType = api.StorageTypeEphemeral
							mariadb.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
						})

						Context("DBPvcSpec is nil", func() {
							BeforeEach(func() {
								mariadb.Spec.Storage = nil
							})

							It("should Handle Job Volume Successfully", shouldHandleJobVolumeSuccessfully)
						})

						Context("DBPvcSpec is given [not nil]", func() {
							BeforeEach(func() {
								mariadb.Spec.Storage = &core.PersistentVolumeClaimSpec{
									Resources: core.ResourceRequirements{
										Requests: core.ResourceList{
											core.ResourceStorage: resource.MustParse(framework.DBPvcStorageSize),
										},
									},
									StorageClassName: types.StringP(root.StorageClass),
								}
							})

							It("should Handle Job Volume Successfully", shouldHandleJobVolumeSuccessfully)
						})
					})
				}
				// End <==============

				// Snapshot PVC Scenarios
				// ==============> Start
				var snapshotPvcScenarios = func() {
					Context("Snapshot PVC is given [not nil]", func() {
						BeforeEach(func() {
							snapshot.Spec.PodVolumeClaimSpec = &core.PersistentVolumeClaimSpec{
								Resources: core.ResourceRequirements{
									Requests: core.ResourceList{
										core.ResourceStorage: resource.MustParse(framework.JobPvcStorageSize),
									},
								},
								StorageClassName: types.StringP(root.StorageClass),
							}
						})

						dbStorageTypeScenarios()
					})

					Context("Snapshot PVC is nil", func() {
						BeforeEach(func() {
							snapshot.Spec.PodVolumeClaimSpec = nil
						})

						dbStorageTypeScenarios()
					})
				}
				// End <==============

				Context("Snapshot StorageType is nil", func() {
					BeforeEach(func() {
						snapshot.Spec.StorageType = nil
					})

					snapshotPvcScenarios()
				})

				Context("Snapshot StorageType is Ephemeral", func() {
					BeforeEach(func() {
						ephemeral := api.StorageTypeEphemeral
						snapshot.Spec.StorageType = &ephemeral
					})

					snapshotPvcScenarios()
				})

				Context("Snapshot StorageType is Durable", func() {
					BeforeEach(func() {
						durable := api.StorageTypeDurable
						snapshot.Spec.StorageType = &durable
					})

					snapshotPvcScenarios()
				})
			})
		})

		Context("Initialize", func() {

			Context("With Script", func() {
				BeforeEach(func() {
					mariadb.Spec.Init = &api.InitSpec{
						ScriptSource: &api.ScriptSourceSpec{
							VolumeSource: core.VolumeSource{
								GitRepo: &core.GitRepoVolumeSource{
									Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
									Directory:  ".",
								},
							},
						},
					}
				})

				It("should run successfully", func() {
					// Create MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("With Snapshot", func() {

				AfterEach(func() {
					// delete snapshot and check for data wipeOut
					deleteSnapshot()

					By("Deleting secret: " + secret.Name)
					err := f.DeleteSecret(secret.ObjectMeta)
					if err != nil && !kerr.IsNotFound(err) {
						Expect(err).NotTo(HaveOccurred())
					}
				})

				var shouldInitializeFromSnapshot = func() {
					// Create MariaDB and take Snapshot
					shouldInsertDataAndTakeSnapshot()

					oldMariaDB, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
					garbageMariaDB.Items = append(garbageMariaDB.Items, *oldMariaDB)

					By("Create mariadb from snapshot")
					mariadb = f.MariaDB()
					mariadb.Spec.Init = &api.InitSpec{
						SnapshotSource: &api.SnapshotSourceSpec{
							Namespace: snapshot.Namespace,
							Name:      snapshot.Name,
						},
					}

					By("Creating init Snapshot Mysql without secret name" + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).Should(HaveOccurred())

					// for snapshot init, user have to use older secret,
					// because the username & password  will be replaced to
					mariadb.Spec.DatabaseSecret = oldMariaDB.Spec.DatabaseSecret

					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				}

				Context("From Local backend", func() {
					var snapPVC *core.PersistentVolumeClaim

					BeforeEach(func() {

						skipDataChecking = true
						snapPVC = f.GetPersistentVolumeClaim()
						err := f.CreatePersistentVolumeClaim(snapPVC)
						Expect(err).NotTo(HaveOccurred())

						secret = f.SecretForLocalBackend()
						snapshot.Spec.DatabaseName = mariadb.Name
						snapshot.Spec.StorageSecretName = secret.Name

						snapshot.Spec.Local = &store.LocalSpec{
							MountPath: "/repo",
							VolumeSource: core.VolumeSource{
								PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
									ClaimName: snapPVC.Name,
								},
							},
						}
					})

					AfterEach(func() {
						err := f.DeletePersistentVolumeClaim(snapPVC.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())
					})

					It("should initialize successfully", shouldInitializeFromSnapshot)
				})

				Context("From GCS backend", func() {

					BeforeEach(func() {

						skipDataChecking = false
						secret = f.SecretForGCSBackend()
						snapshot.Spec.StorageSecretName = secret.Name
						snapshot.Spec.DatabaseName = mariadb.Name

						snapshot.Spec.GCS = &store.GCSSpec{
							Bucket: os.Getenv(GCS_BUCKET_NAME),
						}
					})

					It("should initialize successfully", shouldInitializeFromSnapshot)
				})
			})

			// To run this test,
			// 1st: Deploy stash latest operator
			// 2nd: create mariadb related tasks and functions from
			// `github.com/kubedb/mariadb/hack/dev/examples/stash01_config.yaml`
			Context("With Stash/Restic", func() {
				var bc *stashV1beta1.BackupConfiguration
				var bs *stashV1beta1.BackupSession
				var rs *stashV1beta1.RestoreSession
				var repo *stashV1alpha1.Repository

				BeforeEach(func() {
					skipDataChecking = true
					if !f.FoundStashCRDs() {
						Skip("Skipping tests for stash integration. reason: stash operator is not running.")
					}
				})

				AfterEach(func() {
					By("Deleting BackupConfiguration")
					err := f.DeleteBackupConfiguration(bc.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Deleting BackupSession")
					err = f.DeleteBackupSession(bs.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Deleting RestoreSession")
					err = f.DeleteRestoreSession(rs.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Deleting Repository")
					err = f.DeleteRepository(repo.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Deleting Stash RBACs")
					err = f.DeleteStashMariaDBRBAC(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
				})

				var createAndWaitForInitializing = func() {
					By("Creating MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Initializing mariadb")
					f.EventuallyMariaDBPhase(mariadb.ObjectMeta).Should(Equal(api.DatabasePhaseInitializing))

					By("Wait for AppBinding to create")
					f.EventuallyAppBinding(mariadb.ObjectMeta).Should(BeTrue())

					By("Check valid AppBinding Specs")
					err = f.CheckAppBindingSpec(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Waiting for database to be ready")
					f.EventuallyDatabaseReady(mariadb.ObjectMeta, dbName).Should(BeTrue())
				}

				var shouldInitializeFromStash = func() {
					By("Ensuring Stash RBACs")
					err := f.EnsureStashMariaDBRBAC(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Creating Table")
					f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

					By("Inserting Rows")
					f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Create Secret")
					err = f.CreateSecret(secret)
					Expect(err).NotTo(HaveOccurred())

					By("Create Repositories")
					err = f.CreateRepository(repo)
					Expect(err).NotTo(HaveOccurred())

					By("Create BackupConfiguration")
					err = f.CreateBackupConfiguration(bc)
					Expect(err).NotTo(HaveOccurred())

					By("Create BackupSession")
					err = f.CreateBackupSession(bs)
					Expect(err).NotTo(HaveOccurred())

					// eventually backupsession succeeded
					By("Check for Succeeded backupsession")
					f.EventuallyBackupSessionPhase(bs.ObjectMeta).Should(Equal(stashV1beta1.BackupSessionSucceeded))

					oldMariaDB, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					garbageMariaDB.Items = append(garbageMariaDB.Items, *oldMariaDB)

					By("Create mariadb from stash")
					*mariadb = *f.MariaDB()
					rs = f.RestoreSession(mariadb.ObjectMeta, oldMariaDB.ObjectMeta)
					mariadb.Spec.DatabaseSecret = oldMariaDB.Spec.DatabaseSecret
					mariadb.Spec.Init = &api.InitSpec{
						StashRestoreSession: &core.LocalObjectReference{
							Name: rs.Name,
						},
					}

					// Create and wait for running MariaDB
					createAndWaitForInitializing()

					By("Create RestoreSession")
					err = f.CreateRestoreSession(rs)
					Expect(err).NotTo(HaveOccurred())

					// eventually backupsession succeeded
					By("Check for Succeeded restoreSession")
					f.EventuallyRestoreSessionPhase(rs.ObjectMeta).Should(Equal(stashV1beta1.RestoreSessionSucceeded))

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				}

				Context("From GCS backend", func() {

					BeforeEach(func() {
						secret = f.SecretForGCSBackend()
						secret = f.PatchSecretForRestic(secret)
						bc = f.BackupConfiguration(mariadb.ObjectMeta)
						bs = f.BackupSession(mariadb.ObjectMeta)
						repo = f.Repository(mariadb.ObjectMeta, secret.Name)

						repo.Spec.Backend = store.Backend{
							GCS: &store.GCSSpec{
								Bucket: os.Getenv("GCS_BUCKET_NAME"),
								Prefix: fmt.Sprintf("stash/%v/%v", mariadb.Namespace, mariadb.Name),
							},
							StorageSecretName: secret.Name,
						}
					})

					It("should run successfully", shouldInitializeFromStash)
				})

			})

		})

		Context("Resume", func() {

			Context("Super Fast User - Create-Delete-Create-Delete-Create ", func() {
				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Creating Table")
					f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

					By("Inserting Row")
					f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					// Delete without caring if DB is resumed
					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for MariaDB to be deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for DormantDatabase to be deleted")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("Without Init", func() {
				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Creating Table")
					f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

					By("Inserting Row")
					f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for DormantDatabase to be deleted")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("with init Script", func() {
				BeforeEach(func() {
					mariadb.Spec.Init = &api.InitSpec{
						ScriptSource: &api.ScriptSourceSpec{
							VolumeSource: core.VolumeSource{
								GitRepo: &core.GitRepoVolumeSource{
									Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
									Directory:  ".",
								},
							},
						},
					}
				})

				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for DormantDatabase to be deleted")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					mariadb, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
					Expect(mariadb.Spec.Init).NotTo(BeNil())

					By("Checking MariaDB crd does not have kubedb.com/initialized annotation")
					_, err = meta_util.GetString(mariadb.Annotations, api.AnnotationInitialized)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("With Snapshot Init", func() {

				AfterEach(func() {
					// delete snapshot and check for data wipeOut
					deleteSnapshot()

					By("Deleting secret: " + secret.Name)
					err := f.DeleteSecret(secret.ObjectMeta)
					if err != nil && !kerr.IsNotFound(err) {
						Expect(err).NotTo(HaveOccurred())
					}
				})

				BeforeEach(func() {
					skipDataChecking = false
					secret = f.SecretForGCSBackend()
					snapshot.Spec.StorageSecretName = secret.Name
					snapshot.Spec.GCS = &store.GCSSpec{
						Bucket: os.Getenv(GCS_BUCKET_NAME),
					}
					snapshot.Spec.DatabaseName = mariadb.Name
				})

				It("should resume successfully", func() {
					// Create MariaDB and take Snapshot
					shouldInsertDataAndTakeSnapshot()

					oldMariaDB, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					garbageMariaDB.Items = append(garbageMariaDB.Items, *oldMariaDB)

					By("Create mariadb from snapshot")
					mariadb = f.MariaDB()
					mariadb.Spec.Init = &api.InitSpec{
						SnapshotSource: &api.SnapshotSourceSpec{
							Namespace: snapshot.Namespace,
							Name:      snapshot.Name,
						},
					}

					By("Creating MariaDB without secret name to init from Snapshot: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).Should(HaveOccurred())

					// for snapshot init, user have to use older secret,
					// because the username & password  will be replaced to
					mariadb.Spec.DatabaseSecret = oldMariaDB.Spec.DatabaseSecret

					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for DormantDatabase to be deleted")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					mariadb, err = f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
					Expect(mariadb.Spec.Init).ShouldNot(BeNil())

					By("Checking MariaDB has kubedb.com/initialized annotation")
					_, err = meta_util.GetString(mariadb.Annotations, api.AnnotationInitialized)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("Multiple times with init", func() {

				BeforeEach(func() {
					mariadb.Spec.Init = &api.InitSpec{
						ScriptSource: &api.ScriptSourceSpec{
							VolumeSource: core.VolumeSource{
								GitRepo: &core.GitRepoVolumeSource{
									Repository: "https://github.com/kubedb/mariadb-init-scripts.git",
									Directory:  ".",
								},
							},
						},
					}
				})

				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					for i := 0; i < 3; i++ {
						By(fmt.Sprintf("%v-th", i+1) + " time running.")

						By("Delete mariadb")
						err = f.DeleteMariaDB(mariadb.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Wait for mariadb to be paused")
						f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

						// Create MariaDB object again to resume it
						By("Create MariaDB: " + mariadb.Name)
						err = f.CreateMariaDB(mariadb)
						Expect(err).NotTo(HaveOccurred())

						By("Wait for DormantDatabase to be deleted")
						f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

						By("Wait for Running mariadb")
						f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

						By("Checking Row Count of Table")
						f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

						mariadb, err := f.GetMariaDB(mariadb.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())
						Expect(mariadb.Spec.Init).ShouldNot(BeNil())

						By("Checking MariaDB crd does not have kubedb.com/initialized annotation")
						_, err = meta_util.GetString(mariadb.Annotations, api.AnnotationInitialized)
						Expect(err).To(HaveOccurred())
					}
				})
			})
		})

		Context("SnapshotScheduler", func() {

			BeforeEach(func() {
				skipDataChecking = false
			})

			AfterEach(func() {
				snapshotList, err := f.GetSnapshotList(mariadb.ObjectMeta)
				Expect(err).NotTo(HaveOccurred())

				for _, snap := range snapshotList.Items {
					snapshot = &snap

					// delete snapshot and check for data wipeOut
					deleteSnapshot()
				}

				By("Deleting secret: " + secret.Name)
				err = f.DeleteSecret(secret.ObjectMeta)
				if err != nil && !kerr.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred())
				}
			})

			Context("With Startup", func() {

				var shouldStartupSchedular = func() {
					By("Create Secret")
					err := f.CreateSecret(secret)
					Expect(err).NotTo(HaveOccurred())

					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Count multiple Snapshot Object")
					f.EventuallySnapshotCount(mariadb.ObjectMeta).Should(matcher.MoreThan(3))

					By("Remove Backup Scheduler from MariaDB")
					_, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.BackupSchedule = nil
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Verify multiple Succeeded Snapshot")
					f.EventuallyMultipleSnapshotFinishedProcessing(mariadb.ObjectMeta).Should(Succeed())
				}

				Context("with local", func() {
					BeforeEach(func() {
						skipDataChecking = true
						secret = f.SecretForLocalBackend()
						mariadb.Spec.BackupSchedule = &api.BackupScheduleSpec{
							CronExpression: "@every 20s",
							Backend: store.Backend{
								StorageSecretName: secret.Name,
								Local: &store.LocalSpec{
									MountPath: "/repo",
									VolumeSource: core.VolumeSource{
										EmptyDir: &core.EmptyDirVolumeSource{},
									},
								},
							},
						}
					})

					It("should run scheduler successfully", shouldStartupSchedular)
				})

				Context("with GCS", func() {
					BeforeEach(func() {
						secret = f.SecretForGCSBackend()
						mariadb.Spec.BackupSchedule = &api.BackupScheduleSpec{
							CronExpression: "@every 1m",
							Backend: store.Backend{
								StorageSecretName: secret.Name,
								GCS: &store.GCSSpec{
									Bucket: os.Getenv(GCS_BUCKET_NAME),
								},
							},
						}
					})

					It("should run scheduler successfully", shouldStartupSchedular)
				})
			})

			Context("With Update - with Local", func() {

				BeforeEach(func() {
					skipDataChecking = true
					secret = f.SecretForLocalBackend()
				})

				It("should run scheduler successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Create Secret")
					err := f.CreateSecret(secret)
					Expect(err).NotTo(HaveOccurred())

					By("Update mariadb")
					_, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.BackupSchedule = &api.BackupScheduleSpec{
							CronExpression: "@every 20s",
							Backend: store.Backend{
								StorageSecretName: secret.Name,
								Local: &store.LocalSpec{
									MountPath: "/repo",
									VolumeSource: core.VolumeSource{
										EmptyDir: &core.EmptyDirVolumeSource{},
									},
								},
							},
						}
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Count multiple Snapshot Object")
					f.EventuallySnapshotCount(mariadb.ObjectMeta).Should(matcher.MoreThan(3))

					By("Remove Backup Scheduler from MariaDB")
					_, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.BackupSchedule = nil
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Verify multiple Succeeded Snapshot")
					f.EventuallyMultipleSnapshotFinishedProcessing(mariadb.ObjectMeta).Should(Succeed())
				})
			})

			Context("Re-Use DormantDatabase's scheduler", func() {

				BeforeEach(func() {
					skipDataChecking = true
					secret = f.SecretForLocalBackend()
				})

				It("should re-use scheduler successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Create Secret")
					err := f.CreateSecret(secret)
					Expect(err).NotTo(HaveOccurred())

					By("Update mariadb")
					_, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.BackupSchedule = &api.BackupScheduleSpec{
							CronExpression: "@every 20s",
							Backend: store.Backend{
								StorageSecretName: secret.Name,
								Local: &store.LocalSpec{
									MountPath: "/repo",
									VolumeSource: core.VolumeSource{
										EmptyDir: &core.EmptyDirVolumeSource{},
									},
								},
							},
						}
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Creating Table")
					f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

					By("Inserting Row")
					f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Count multiple Snapshot Object")
					f.EventuallySnapshotCount(mariadb.ObjectMeta).Should(matcher.MoreThan(3))

					By("Verify multiple Succeeded Snapshot")
					f.EventuallyMultipleSnapshotFinishedProcessing(mariadb.ObjectMeta).Should(Succeed())

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for DormantDatabase to be deleted")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Count multiple Snapshot Object")
					f.EventuallySnapshotCount(mariadb.ObjectMeta).Should(matcher.MoreThan(5))

					By("Remove Backup Scheduler from MariaDB")
					_, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.BackupSchedule = nil
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Verify multiple Succeeded Snapshot")
					f.EventuallyMultipleSnapshotFinishedProcessing(mariadb.ObjectMeta).Should(Succeed())
				})
			})
		})

		Context("Termination Policy", func() {

			BeforeEach(func() {
				skipDataChecking = false
				secret = f.SecretForGCSBackend()
				snapshot.Spec.StorageSecretName = secret.Name
				snapshot.Spec.GCS = &store.GCSSpec{
					Bucket: os.Getenv(GCS_BUCKET_NAME),
				}
				snapshot.Spec.DatabaseName = mariadb.Name
			})

			Context("with TerminationDoNotTerminate", func() {
				BeforeEach(func() {
					skipDataChecking = true
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyDoNotTerminate
				})

				It("should work successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).Should(HaveOccurred())

					By("MariaDB is not paused. Check for mariadb")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeTrue())

					By("Check for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Update mariadb to set spec.terminationPolicy = Pause")
					_, err := f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.TerminationPolicy = api.TerminationPolicyPause
						return in
					})
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("with TerminationPolicyPause (default)", func() {

				AfterEach(func() {
					// delete snapshot and check for data wipeOut
					deleteSnapshot()

					By("Deleting secret: " + secret.Name)
					err := f.DeleteSecret(secret.ObjectMeta)
					if err != nil && !kerr.IsNotFound(err) {
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("should create DormantDatabase and resume from it", func() {
					// Run MariaDB and take snapshot
					shouldInsertDataAndTakeSnapshot()

					By("Deleting MariaDB crd")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					// DormantDatabase.Status= paused, means mariadb object is deleted
					By("Waiting for mariadb to be paused")
					f.EventuallyDormantDatabaseStatus(mariadb.ObjectMeta).Should(matcher.HavePaused())

					By("Checking PVC hasn't been deleted")
					f.EventuallyPVCCount(mariadb.ObjectMeta).Should(Equal(1))

					By("Checking Secret hasn't been deleted")
					f.EventuallyDBSecretCount(mariadb.ObjectMeta).Should(Equal(1))

					By("Checking snapshot hasn't been deleted")
					f.EventuallySnapshot(snapshot.ObjectMeta).Should(BeTrue())

					if !skipDataChecking {
						By("Check for snapshot data")
						f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
					}

					// Create MariaDB object again to resume it
					By("Create (resume) MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for DormantDatabase to be deleted")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Wait for Running mariadb")
					f.EventuallyMariaDBRunning(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking row count of table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("with TerminationPolicyDelete", func() {

				BeforeEach(func() {
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyDelete
				})

				AfterEach(func() {
					// delete snapshot and check for data wipeOut
					deleteSnapshot()

					By("Deleting secret: " + secret.Name)
					err := f.DeleteSecret(secret.ObjectMeta)
					if err != nil && !kerr.IsNotFound(err) {
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("should not create DormantDatabase and should not delete secret and snapshot", func() {
					// Run MariaDB and take snapshot
					shouldInsertDataAndTakeSnapshot()

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("wait until mariadb is deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking DormantDatabase is not created")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking PVC has been deleted")
					f.EventuallyPVCCount(mariadb.ObjectMeta).Should(Equal(0))

					By("Checking Secret hasn't been deleted")
					f.EventuallyDBSecretCount(mariadb.ObjectMeta).Should(Equal(1))

					By("Checking Snapshot hasn't been deleted")
					f.EventuallySnapshot(snapshot.ObjectMeta).Should(BeTrue())

					if !skipDataChecking {
						By("Check for intact snapshot data")
						f.EventuallySnapshotDataFound(snapshot).Should(BeTrue())
					}
				})
			})

			Context("with TerminationPolicyWipeOut", func() {

				BeforeEach(func() {
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
				})

				It("should not create DormantDatabase and should wipeOut all", func() {
					// Run MariaDB and take snapshot
					shouldInsertDataAndTakeSnapshot()

					By("Delete mariadb")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("wait until mariadb is deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking DormantDatabase is not created")
					f.EventuallyDormantDatabase(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking PVCs has been deleted")
					f.EventuallyPVCCount(mariadb.ObjectMeta).Should(Equal(0))

					By("Checking Snapshots has been deleted")
					f.EventuallySnapshot(snapshot.ObjectMeta).Should(BeFalse())

					By("Checking Secrets has been deleted")
					f.EventuallyDBSecretCount(mariadb.ObjectMeta).Should(Equal(0))
				})
			})
		})

		Context("EnvVars", func() {

			Context("Database Name as EnvVar", func() {

				It("should create DB with name provided in EvnVar", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					dbName = f.App()
					mariadb.Spec.PodTemplate.Spec.Env = []core.EnvVar{
						{
							Name:  MARIADB_DATABASE,
							Value: dbName,
						},
					}
					//test general behaviour
					testGeneralBehaviour()
				})
			})

			Context("Root Password as EnvVar", func() {

				It("should reject to create MariaDB CRD", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					mariadb.Spec.PodTemplate.Spec.Env = []core.EnvVar{
						{
							Name:  MARIADB_ROOT_PASSWORD,
							Value: "not@secret",
						},
					}
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("Update EnvVar", func() {

				It("should not reject to update EvnVar", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					dbName = f.App()
					mariadb.Spec.PodTemplate.Spec.Env = []core.EnvVar{
						{
							Name:  MARIADB_DATABASE,
							Value: dbName,
						},
					}
					//test general behaviour
					testGeneralBehaviour()

					By("Patching EnvVar")
					_, _, err = util.PatchMariaDB(f.ExtClient().KubedbV1alpha1(), mariadb, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.PodTemplate.Spec.Env = []core.EnvVar{
							{
								Name:  MARIADB_DATABASE,
								Value: "patched-db",
							},
						}
						return in
					})
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("Custom config", func() {

			customConfigs := []string{
				"max_connections=200",
				"read_buffer_size=1048576", // 1MB
			}

			Context("from configMap", func() {
				var userConfig *core.ConfigMap

				BeforeEach(func() {
					userConfig = f.GetCustomConfig(customConfigs)
				})

				AfterEach(func() {
					By("Deleting configMap: " + userConfig.Name)
					err := f.DeleteConfigMap(userConfig.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

				})

				It("should set configuration provided in configMap", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					By("Creating configMap: " + userConfig.Name)
					err := f.CreateConfigMap(userConfig)
					Expect(err).NotTo(HaveOccurred())

					mariadb.Spec.ConfigSource = &core.VolumeSource{
						ConfigMap: &core.ConfigMapVolumeSource{
							LocalObjectReference: core.LocalObjectReference{
								Name: userConfig.Name,
							},
						},
					}

					// Create MariaDB
					createAndWaitForRunning()

					By("Checking mariadb configured from provided custom configuration")
					for _, cfg := range customConfigs {
						f.EventuallyMariaDBVariable(mariadb.ObjectMeta, dbName, cfg).Should(matcher.UseCustomConfig(cfg))
					}
				})
			})
		})

		Context("StorageType ", func() {

			var shouldRunSuccessfully = func() {

				if skipMessage != "" {
					Skip(skipMessage)
				}

				// Create MariaDB
				createAndWaitForRunning()

				By("Creating Table")
				f.EventuallyCreateTable(mariadb.ObjectMeta, dbName).Should(BeTrue())

				By("Inserting Rows")
				f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

				By("Checking Row Count of Table")
				f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
			}

			Context("Ephemeral", func() {

				Context("General Behaviour", func() {

					BeforeEach(func() {
						mariadb.Spec.StorageType = api.StorageTypeEphemeral
						mariadb.Spec.Storage = nil
						mariadb.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
					})

					It("should run successfully", shouldRunSuccessfully)
				})

				Context("With TerminationPolicyPause", func() {

					BeforeEach(func() {
						mariadb.Spec.StorageType = api.StorageTypeEphemeral
						mariadb.Spec.Storage = nil
						mariadb.Spec.TerminationPolicy = api.TerminationPolicyPause
					})

					It("should reject to create MariaDB object", func() {

						By("Creating MariaDB: " + mariadb.Name)
						err := f.CreateMariaDB(mariadb)
						Expect(err).To(HaveOccurred())
					})
				})
			})
		})
	})
})
