package cmds

import (
	"io"

	"github.com/appscode/go/log"
	"github.com/kubedb/mariadb/pkg/cmds/server"
	"github.com/spf13/cobra"
	"kmodules.xyz/client-go/meta"
	"kmodules.xyz/client-go/tools/cli"
)

func NewCmdRun(version string, out, errOut io.Writer, stopCh <-chan struct{}) *cobra.Command {
	o := server.NewMySQLServerOptions(out, errOut)

	cmd := &cobra.Command{
		Use:               "run",
		Short:             "Launch MySQL server",
		DisableAutoGenTag: true,
		PreRun: func(c *cobra.Command, args []string) {
			cli.SendPeriodicAnalytics(c, version)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			log.Infoln("Starting mysql-server...")

			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(args); err != nil {
				return err
			}
			if err := o.Run(stopCh); err != nil {
				return err
			}
			return nil
		},
	}

	o.AddFlags(cmd.Flags())
	meta.AddLabelBlacklistFlag(cmd.Flags())

	return cmd
}
