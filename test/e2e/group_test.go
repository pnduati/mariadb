package e2e_test

import (
	"fmt"

	"github.com/appscode/go/log"
	"github.com/appscode/go/types"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/mariadb/test/e2e/framework"
	"github.com/kubedb/mariadb/test/e2e/matcher"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kerr "k8s.io/apimachinery/pkg/api/errors"
)

var _ = Describe("MariaDB Group Replication Tests", func() {
	var (
		err            error
		f              *framework.Invocation
		mariadb        *api.MariaDB
		garbageMariaDB *api.MariaDBList
		//skipMessage string
		dbName       string
		dbNameKubedb string
	)

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
	var writeOnPrimary = func(primaryPodIndex int) {
		By(fmt.Sprintf("Write on primary '%s-%d'", mariadb.Name, primaryPodIndex))
		f.EventuallyCreateDatabase(mariadb.ObjectMeta, dbName).Should(BeTrue())
		f.EventuallyCreateTable(mariadb.ObjectMeta, dbNameKubedb).Should(BeTrue())
		rowCnt := 1
		f.EventuallyInsertRow(mariadb.ObjectMeta, dbNameKubedb, primaryPodIndex, rowCnt).Should(BeTrue())
		f.EventuallyCountRow(mariadb.ObjectMeta, dbNameKubedb, primaryPodIndex).Should(Equal(rowCnt))
	}
	var CheckDBVersionForGroupReplication = func() {
		if framework.DBCatalogName != "5.7.25" && framework.DBCatalogName != "5.7-v1" {
			Skip("For group replication CheckDBVersionForGroupReplication, DB version must be one of '5.7.25' or '5.7-v1'")
		}
	}

	BeforeEach(func() {
		f = root.Invoke()
		mariadb = f.MariaDBGroup()
		garbageMariaDB = new(api.MariaDBList)
		//skipMessage = ""
		dbName = "mariadb"
		dbNameKubedb = "kubedb"

		CheckDBVersionForGroupReplication()
	})

	Context("Behaviour tests", func() {
		BeforeEach(func() {
			createAndWaitForRunning()
		})

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

		It("should be possible to create a basic 3 member group", func() {
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)
			rowCnt := 1
			primaryPodIndex := 0
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				if i == primaryPodIndex {
					continue
				}

				By(fmt.Sprintf("Write on secondary '%s-%d'", mariadb.Name, i))
				f.InsertRowFromSecondary(mariadb.ObjectMeta, dbNameKubedb, i).Should(BeFalse())

				By(fmt.Sprintf("Read from secondary '%s-%d'", mariadb.Name, i))
				f.EventuallyCountRow(mariadb.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})

		It("should failover successfully", func() {
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)

			By(fmt.Sprintf("Taking down the primary '%s-%d'", mariadb.Name, 0))
			err = f.RemoverPrimaryToFailover(mariadb.ObjectMeta, 0)
			Expect(err).NotTo(HaveOccurred())

			By("Checking status after failover")
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(
					Or(
						Equal(1),
						Equal(2),
					),
				)
			}

			By("Checking for data after failover")
			rowCnt := 1
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				By(fmt.Sprintf("Read from '%s-%d'", mariadb.Name, i))
				f.EventuallyCountRow(mariadb.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})

		It("should be possible to scale up", func() {
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(Equal(0))
			}

			By("Scaling up")
			mariadb, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
				in.Spec.Replicas = types.Int32P(api.MariaDBDefaultGroupSize + 1)

				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Wait for new member to be ready")
			Expect(f.WaitUntilPodRunningBySelector(mariadb)).NotTo(HaveOccurred())

			By("Checking status after scaling up")
			for i := 0; i < api.MariaDBDefaultGroupSize+1; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize + 1))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)

			primaryPodIndex := 0
			rowCnt := 1
			for i := 0; i < api.MariaDBDefaultGroupSize+1; i++ {
				if i == primaryPodIndex {
					continue
				}

				By(fmt.Sprintf("Write on secondary '%s-%d'", mariadb.Name, i))
				f.InsertRowFromSecondary(mariadb.ObjectMeta, dbNameKubedb, i).Should(BeFalse())

				By(fmt.Sprintf("Read from secondary '%s-%d'", mariadb.Name, i))
				f.EventuallyCountRow(mariadb.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})

		It("Should be possible to scale down", func() {
			for i := 0; i < api.MariaDBDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(Equal(0))
			}

			By("Scaling down")
			mariadb, err = f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
				in.Spec.Replicas = types.Int32P(api.MariaDBDefaultGroupSize - 1)

				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for all member to be ready")
			Expect(f.WaitUntilPodRunningBySelector(mariadb)).NotTo(HaveOccurred())

			By("Checking status after scaling down")
			for i := 0; i < api.MariaDBDefaultGroupSize-1; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyONLINEMembersCount(mariadb.ObjectMeta, dbName, i).Should(Equal(api.MariaDBDefaultGroupSize - 1))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mariadb.Name, i))
				f.EventuallyGetPrimaryHostIndex(mariadb.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)

			primaryPodIndex := 0
			rowCnt := 1
			for i := 0; i < api.MariaDBDefaultGroupSize-1; i++ {
				if i == primaryPodIndex {
					continue
				}

				By(fmt.Sprintf("Write on secondary '%s-%d'", mariadb.Name, i))
				f.InsertRowFromSecondary(mariadb.ObjectMeta, dbNameKubedb, i).Should(BeFalse())

				By(fmt.Sprintf("Read from secondary '%s-%d'", mariadb.Name, i))
				f.EventuallyCountRow(mariadb.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})
	})

	Context("PDB", func() {

		It("should run evictions successfully", func() {
			// Create MariaDB
			By("Create and run MariaDB Group with three replicas")
			createAndWaitForRunning()
			//Evict MariaDB pods
			By("Try to evict pods")
			err := f.EvictPodsFromStatefulSet(mariadb.ObjectMeta)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
