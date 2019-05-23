package controller

import (
	"fmt"

	"github.com/appscode/go/log"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kutil "kmodules.xyz/client-go"
	core_util "kmodules.xyz/client-go/core/v1"
	meta_util "kmodules.xyz/client-go/meta"
	"kmodules.xyz/monitoring-agent-api/agents"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
)

func (c *Controller) newMonitorController(mariadb *api.MariaDB) (mona.Agent, error) {
	monitorSpec := mariadb.Spec.Monitor

	if monitorSpec == nil {
		return nil, fmt.Errorf("MonitorSpec not found for MariaDB %v/%v in %v", mariadb.Namespace, mariadb.Name, mariadb.Spec)
	}

	if monitorSpec.Prometheus != nil {
		return agents.New(monitorSpec.Agent, c.Client, c.ApiExtKubeClient, c.promClient), nil
	}

	return nil, fmt.Errorf("monitoring controller not found for MariaDB %v/%v in %v", mariadb.Namespace, mariadb.Name, monitorSpec)
}

func (c *Controller) addOrUpdateMonitor(mariadb *api.MariaDB) (kutil.VerbType, error) {
	agent, err := c.newMonitorController(mariadb)
	if err != nil {
		return kutil.VerbUnchanged, err
	}
	return agent.CreateOrUpdate(mariadb.StatsService(), mariadb.Spec.Monitor)
}

func (c *Controller) deleteMonitor(mariadb *api.MariaDB) (kutil.VerbType, error) {
	agent, err := c.newMonitorController(mariadb)
	if err != nil {
		return kutil.VerbUnchanged, err
	}
	return agent.Delete(mariadb.StatsService())
}

func (c *Controller) getOldAgent(mariadb *api.MariaDB) mona.Agent {
	service, err := c.Client.CoreV1().Services(mariadb.Namespace).Get(mariadb.StatsService().ServiceName(), metav1.GetOptions{})
	if err != nil {
		return nil
	}
	oldAgentType, _ := meta_util.GetStringValue(service.Annotations, mona.KeyAgent)
	return agents.New(mona.AgentType(oldAgentType), c.Client, c.ApiExtKubeClient, c.promClient)
}

func (c *Controller) setNewAgent(mariadb *api.MariaDB) error {
	service, err := c.Client.CoreV1().Services(mariadb.Namespace).Get(mariadb.StatsService().ServiceName(), metav1.GetOptions{})
	if err != nil {
		return err
	}
	_, _, err = core_util.PatchService(c.Client, service, func(in *core.Service) *core.Service {
		in.Annotations = core_util.UpsertMap(in.Annotations, map[string]string{
			mona.KeyAgent: string(mariadb.Spec.Monitor.Agent),
		},
		)
		return in
	})
	return err
}

func (c *Controller) manageMonitor(mariadb *api.MariaDB) error {
	oldAgent := c.getOldAgent(mariadb)
	if mariadb.Spec.Monitor != nil {
		if oldAgent != nil &&
			oldAgent.GetType() != mariadb.Spec.Monitor.Agent {
			if _, err := oldAgent.Delete(mariadb.StatsService()); err != nil {
				log.Errorf("error in deleting Prometheus agent. Reason: %s", err)
			}
		}
		if _, err := c.addOrUpdateMonitor(mariadb); err != nil {
			return err
		}
		return c.setNewAgent(mariadb)
	} else if oldAgent != nil {
		if _, err := oldAgent.Delete(mariadb.StatsService()); err != nil {
			log.Errorf("error in deleting Prometheus agent. Reason: %s", err)
		}
	}
	return nil
}
