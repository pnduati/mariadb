package framework

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"kmodules.xyz/client-go/tools/portforward"
)

type KubedbTable struct {
	Id   int64
	Name string
}

func (f *Framework) forwardPort(meta metav1.ObjectMeta, clientPodIndex int) (*portforward.Tunnel, error) {
	clientPodName := fmt.Sprintf("%v-%d", meta.Name, clientPodIndex)
	tunnel := portforward.NewTunnel(
		f.kubeClient.CoreV1().RESTClient(),
		f.restConfig,
		meta.Namespace,
		clientPodName,
		3306,
	)

	if err := tunnel.ForwardPort(); err != nil {
		return nil, err
	}
	return tunnel, nil
}

func (f *Framework) getMariaDBClient(meta metav1.ObjectMeta, tunnel *portforward.Tunnel, dbName string) (*xorm.Engine, error) {
	mariadb, err := f.GetMariaDB(meta)
	if err != nil {
		return nil, err
	}
	pass, err := f.GetMariaDBRootPassword(mariadb)
	if err != nil {
		return nil, err
	}

	cnnstr := fmt.Sprintf("root:%v@tcp(127.0.0.1:%v)/%s", pass, tunnel.Local, dbName)
	return xorm.NewEngine("mariadb", cnnstr)
}

func (f *Framework) EventuallyDatabaseReady(meta metav1.ObjectMeta, dbName string) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			tunnel, err := f.forwardPort(meta, 0)
			if err != nil {
				return false
			}
			defer tunnel.Close()

			en, err := f.getMariaDBClient(meta, tunnel, dbName)
			if err != nil {
				return false
			}
			defer en.Close()

			if err := en.Ping(); err != nil {
				return false
			}
			return true
		},
		time.Minute*10,
		time.Second*20,
	)
	return nil
}

func (f *Framework) EventuallyCreateTable(meta metav1.ObjectMeta, dbName string) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			tunnel, err := f.forwardPort(meta, 0)
			if err != nil {
				return false
			}
			defer tunnel.Close()

			en, err := f.getMariaDBClient(meta, tunnel, dbName)
			if err != nil {
				return false
			}
			defer en.Close()

			if err := en.Ping(); err != nil {
				return false
			}

			err = en.Sync(new(KubedbTable))
			if err != nil {
				fmt.Println("creation error", err)
				return false
			}
			return true
		},
		time.Minute*10,
		time.Second*20,
	)
	return nil
}

func (f *Framework) EventuallyInsertRow(meta metav1.ObjectMeta, dbName string, clientPodIndex, total int) GomegaAsyncAssertion {
	count := 0
	return Eventually(
		func() bool {
			tunnel, err := f.forwardPort(meta, clientPodIndex)
			if err != nil {
				return false
			}
			defer tunnel.Close()

			en, err := f.getMariaDBClient(meta, tunnel, dbName)
			if err != nil {
				return false
			}
			defer en.Close()

			if err := en.Ping(); err != nil {
				return false
			}

			for i := count; i < total; i++ {
				if _, err := en.Insert(&KubedbTable{
					Name: fmt.Sprintf("KubedbName-%v", i),
				}); err != nil {
					return false
				}
				count++
			}
			return true
		},
		time.Minute*10,
		time.Second*10,
	)
	return nil
}

func (f *Framework) EventuallyCountRow(meta metav1.ObjectMeta, dbName string, clientPodIndex int) GomegaAsyncAssertion {
	return Eventually(
		func() int {
			tunnel, err := f.forwardPort(meta, clientPodIndex)
			if err != nil {
				return -1
			}
			defer tunnel.Close()

			en, err := f.getMariaDBClient(meta, tunnel, dbName)
			if err != nil {
				return -1
			}
			defer en.Close()

			if err := en.Ping(); err != nil {
				return -1
			}

			kubedb := new(KubedbTable)
			total, err := en.Count(kubedb)
			if err != nil {
				return -1
			}
			return int(total)
		},
		time.Minute*10,
		time.Second*20,
	)
}

func (f *Framework) EventuallyMariaDBVariable(meta metav1.ObjectMeta, dbName string, config string) GomegaAsyncAssertion {
	configPair := strings.Split(config, "=")
	sql := fmt.Sprintf("SHOW VARIABLES LIKE '%s';", configPair[0])
	return Eventually(
		func() []map[string][]byte {
			tunnel, err := f.forwardPort(meta, 0)
			if err != nil {
				return nil
			}
			defer tunnel.Close()

			en, err := f.getMariaDBClient(meta, tunnel, dbName)
			if err != nil {
				return nil
			}
			defer en.Close()

			if err := en.Ping(); err != nil {
				return nil
			}

			results, err := en.Query(sql)
			if err != nil {
				return nil
			}
			return results
		},
		time.Minute*5,
		time.Second*5,
	)
}
