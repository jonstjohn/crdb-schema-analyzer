package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var analyzeFkOrphanCmd = &cobra.Command{
	Use:   "orphan",
	Short: "Analyze Potential FK orphans",
	RunE: func(cmd *cobra.Command, args []string) error {

		analyzer, err := analyze.NewAnalyzer(analyze.AnalyzerConfig{
			DbUrl:    urlFlag,
			Database: databaseFlag,
		})

		if err != nil {
			return err
		}

		filter, err := analyze.NewFKFilter(tablesFlag, constraintsFlag, rulesFlag)
		if err != nil {
			return err
		}
		constraints, err := analyzer.Fks(filter)
		for _, constraint := range constraints {
			if constraint.DeleteRule == analyze.RuleCascade {
				logrus.Infof("Checking for orphaned rows for constraint: %s\n", constraint)
				cnt, err2 := analyzer.FKOrphanedRowCount(constraint)
				if err2 != nil {
					return err2
				}
				if cnt == 0 {
					logrus.Infoln(" -- NONE --")
				} else {
					logrus.Infoln("******************")
					logrus.Infof(" ** %d found **\n", cnt)
					logrus.Infoln("******************")
				}
			}
		}
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	analyzeFkCmd.AddCommand(analyzeFkOrphanCmd)
}
