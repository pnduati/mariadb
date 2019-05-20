package main

import (
	"log"

	"github.com/kubedb/mariadb/pkg/cmds"
	"kmodules.xyz/client-go/logs"
)

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()
	if err := cmds.NewRootCmd(Version).Execute(); err != nil {
		log.Fatal(err)
	}
}
