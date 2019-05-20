package e2e_test

import (
	"fmt"

	"github.com/appscode/go/log"
	"github.com/appscode/go/types"
	catalog "github.com/kubedb/apimachinery/apis/catalog/v1alpha1"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/mariadb/test/e2e/framework"
	"github.com/kubedb/mariadb/test/e2e/matcher"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kerr "k8s.io/apimachinery/pkg/api/errors"
)

var _ = Describe("MySQL Group Replication Tests", func() {
	var (
		err          error
		f            *framework.Invocation
		mysql        *api.MySQL
		garbageMySQL *api.MySQLList
		mysqlVersion *catalog.MySQLVersion
		//skipMessage string
		dbName       string
		dbNameKubedb string
	)

	BeforeEach(func() {
		f = root.Invoke()
		mysql = f.MySQLGroup()
		garbageMySQL = new(api.MySQLList)
		mysqlVersion = f.MySQLVersion()
		//skipMessage = ""
		dbName = "mysql"
		dbNameKubedb = "kubedb"
	})

	var createAndWaitForRunning = func() {
		By("Create MySQLVersion: " + mysqlVersion.Name)
		err = f.CreateMySQLVersion(mysqlVersion)
		Expect(err).NotTo(HaveOccurred())

		By("Create MySQL: " + mysql.Name)
		err = f.CreateMySQL(mysql)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for Running mysql")
		f.EventuallyMySQLRunning(mysql.ObjectMeta).Should(BeTrue())

		By("Wait for AppBinding to create")
		f.EventuallyAppBinding(mysql.ObjectMeta).Should(BeTrue())

		By("Check valid AppBinding Specs")
		err := f.CheckAppBindingSpec(mysql.ObjectMeta)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for database to be ready")
		f.EventuallyDatabaseReady(mysql.ObjectMeta, dbName).Should(BeTrue())
	}
	var deleteTestResource = func() {
		if mysql == nil {
			log.Infoln("Skipping cleanup. Reason: mysql is nil")
			return
		}

		By("Check if mysql " + mysql.Name + " exists.")
		my, err := f.GetMySQL(mysql.ObjectMeta)
		if err != nil {
			if kerr.IsNotFound(err) {
				// MySQL was not created. Hence, rest of cleanup is not necessary.
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}

		By("Delete mysql")
		err = f.DeleteMySQL(mysql.ObjectMeta)
		if err != nil {
			if kerr.IsNotFound(err) {
				log.Infoln("Skipping rest of the cleanup. Reason: MySQL does not exist.")
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}

		if my.Spec.TerminationPolicy == api.TerminationPolicyPause {
			By("Wait for mysql to be paused")
			f.EventuallyDormantDatabaseStatus(mysql.ObjectMeta).Should(matcher.HavePaused())

			By("WipeOut mysql")
			_, err := f.PatchDormantDatabase(mysql.ObjectMeta, func(in *api.DormantDatabase) *api.DormantDatabase {
				in.Spec.WipeOut = true
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Delete Dormant Database")
			err = f.DeleteDormantDatabase(mysql.ObjectMeta)
			Expect(err).NotTo(HaveOccurred())
		}

		By("Wait for mysql resources to be wipedOut")
		f.EventuallyWipedOut(mysql.ObjectMeta).Should(Succeed())
	}
	var writeOnPrimary = func(primaryPodIndex int) {
		By(fmt.Sprintf("Write on primary '%s-%d'", mysql.Name, primaryPodIndex))
		f.EventuallyCreateDatabase(mysql.ObjectMeta, dbName).Should(BeTrue())
		f.EventuallyCreateTable(mysql.ObjectMeta, dbNameKubedb).Should(BeTrue())
		rowCnt := 1
		f.EventuallyInsertRow(mysql.ObjectMeta, dbNameKubedb, primaryPodIndex, rowCnt).Should(BeTrue())
		f.EventuallyCountRow(mysql.ObjectMeta, dbNameKubedb, primaryPodIndex).Should(Equal(rowCnt))
	}

	Context("Behaviour tests", func() {
		BeforeEach(func() {
			createAndWaitForRunning()
		})

		AfterEach(func() {
			// delete resources for current MySQL
			deleteTestResource()

			// old MySQL are in garbageMySQL list. delete their resources.
			for _, my := range garbageMySQL.Items {
				*mysql = my
				deleteTestResource()
			}

			By("Deleting MySQLVersion crd")
			err := f.DeleteMySQLVersion(mysqlVersion.ObjectMeta)
			if err != nil && !kerr.IsNotFound(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("Delete left over workloads if exists any")
			f.CleanWorkloadLeftOvers()
		})

		It("should be possible to create a basic 3 member group", func() {
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)
			rowCnt := 1
			primaryPodIndex := 0
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				if i == primaryPodIndex {
					continue
				}

				By(fmt.Sprintf("Write on secondary '%s-%d'", mysql.Name, i))
				f.InsertRowFromSecondary(mysql.ObjectMeta, dbNameKubedb, i).Should(BeFalse())

				By(fmt.Sprintf("Read from secondary '%s-%d'", mysql.Name, i))
				f.EventuallyCountRow(mysql.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})

		It("should failover successfully", func() {
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)

			By(fmt.Sprintf("Taking down the primary '%s-%d'", mysql.Name, 0))
			err = f.RemoverPrimaryToFailover(mysql.ObjectMeta, 0)
			Expect(err).NotTo(HaveOccurred())

			By("Checking status after failover")
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(
					Or(
						Equal(1),
						Equal(2),
					),
				)
			}

			By("Checking for data after failover")
			rowCnt := 1
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				By(fmt.Sprintf("Read from '%s-%d'", mysql.Name, i))
				f.EventuallyCountRow(mysql.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})

		It("should be possible to scale up", func() {
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(Equal(0))
			}

			By("Scaling up")
			mysql, err = f.PatchMySQL(mysql.ObjectMeta, func(in *api.MySQL) *api.MySQL {
				in.Spec.Replicas = types.Int32P(api.MySQLDefaultGroupSize + 1)

				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Wait for new member to be ready")
			Expect(f.WaitUntilPodRunningBySelector(mysql)).NotTo(HaveOccurred())

			By("Checking status after scaling up")
			for i := 0; i < api.MySQLDefaultGroupSize+1; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize + 1))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)

			primaryPodIndex := 0
			rowCnt := 1
			for i := 0; i < api.MySQLDefaultGroupSize+1; i++ {
				if i == primaryPodIndex {
					continue
				}

				By(fmt.Sprintf("Write on secondary '%s-%d'", mysql.Name, i))
				f.InsertRowFromSecondary(mysql.ObjectMeta, dbNameKubedb, i).Should(BeFalse())

				By(fmt.Sprintf("Read from secondary '%s-%d'", mysql.Name, i))
				f.EventuallyCountRow(mysql.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})

		It("Should be possible to scale down", func() {
			for i := 0; i < api.MySQLDefaultGroupSize; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(Equal(0))
			}

			By("Scaling down")
			mysql, err = f.PatchMySQL(mysql.ObjectMeta, func(in *api.MySQL) *api.MySQL {
				in.Spec.Replicas = types.Int32P(api.MySQLDefaultGroupSize - 1)

				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for all member to be ready")
			Expect(f.WaitUntilPodRunningBySelector(mysql)).NotTo(HaveOccurred())

			By("Checking status after scaling down")
			for i := 0; i < api.MySQLDefaultGroupSize-1; i++ {
				By(fmt.Sprintf("Checking ONLINE member count from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyONLINEMembersCount(mysql.ObjectMeta, dbName, i).Should(Equal(api.MySQLDefaultGroupSize - 1))

				By(fmt.Sprintf("Checking primary Pod index from Pod '%s-%d'", mysql.Name, i))
				f.EventuallyGetPrimaryHostIndex(mysql.ObjectMeta, dbName, i).Should(Equal(0))
			}

			writeOnPrimary(0)

			primaryPodIndex := 0
			rowCnt := 1
			for i := 0; i < api.MySQLDefaultGroupSize-1; i++ {
				if i == primaryPodIndex {
					continue
				}

				By(fmt.Sprintf("Write on secondary '%s-%d'", mysql.Name, i))
				f.InsertRowFromSecondary(mysql.ObjectMeta, dbNameKubedb, i).Should(BeFalse())

				By(fmt.Sprintf("Read from secondary '%s-%d'", mysql.Name, i))
				f.EventuallyCountRow(mysql.ObjectMeta, dbNameKubedb, i).Should(Equal(rowCnt))
			}
		})
	})
})
