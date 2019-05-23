package controller

import (
	"github.com/appscode/go/log"
	"github.com/kubedb/apimachinery/apis"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha1/util"
	core_util "kmodules.xyz/client-go/core/v1"
	"kmodules.xyz/client-go/tools/queue"
)

func (c *Controller) initWatcher() {
	c.myInformer = c.KubedbInformerFactory.Kubedb().V1alpha1().MariaDBs().Informer()
	c.myQueue = queue.New("MariaDB", c.MaxNumRequeues, c.NumThreads, c.runMariaDB)
	c.myLister = c.KubedbInformerFactory.Kubedb().V1alpha1().MariaDBs().Lister()
	c.myInformer.AddEventHandler(queue.NewObservableUpdateHandler(c.myQueue.GetQueue(), apis.EnableStatusSubresource))
}

func (c *Controller) runMariaDB(key string) error {
	log.Debugln("started processing, key:", key)
	obj, exists, err := c.myInformer.GetIndexer().GetByKey(key)
	if err != nil {
		log.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}

	if !exists {
		log.Debugf("MariaDB %s does not exist anymore", key)
	} else {
		// Note that you also have to check the uid if you have a local controlled resource, which
		// is dependent on the actual instance, to detect that a MariaDB was recreated with the same name
		mariadb := obj.(*api.MariaDB).DeepCopy()
		if mariadb.DeletionTimestamp != nil {
			if core_util.HasFinalizer(mariadb.ObjectMeta, api.GenericKey) {
				if err := c.terminate(mariadb); err != nil {
					log.Errorln(err)
					return err
				}
				mariadb, _, err = util.PatchMariaDB(c.ExtClient.KubedbV1alpha1(), mariadb, func(in *api.MariaDB) *api.MariaDB {
					in.ObjectMeta = core_util.RemoveFinalizer(in.ObjectMeta, api.GenericKey)
					return in
				})
				return err
			}
		} else {
			mariadb, _, err = util.PatchMariaDB(c.ExtClient.KubedbV1alpha1(), mariadb, func(in *api.MariaDB) *api.MariaDB {
				in.ObjectMeta = core_util.AddFinalizer(in.ObjectMeta, api.GenericKey)
				return in
			})
			if err != nil {
				return err
			}
			if err := c.create(mariadb); err != nil {
				log.Errorln(err)
				c.pushFailureEvent(mariadb, err.Error())
				return err
			}
		}
	}
	return nil
}
