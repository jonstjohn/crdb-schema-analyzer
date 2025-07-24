package cmd

import (
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var sqlFlag bool

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
			logrus.Infof("Checking for orphaned rows for constraint: %s\n", constraint)
			var cnt int
			var sqls []string
			if sqlFlag {
				orphans, err2 := analyzer.FKOrphans(constraint)
				if err2 != nil {
					return err2
				}
				for _, orphan := range orphans {
					sql, err := orphan.Sql()
					if err != nil {
						return err
					}
					sqls = append(sqls, sql)
				}
				cnt = len(orphans)
			} else {
				cnt, err = analyzer.FKOrphanedRowCount(constraint)
				if err != nil {
					return err
				}
			}
			if cnt == 0 {
				logrus.Infoln(" -- NONE --")
			} else {
				logrus.Infoln("******************")
				logrus.Infof(" ** %d found **\n", cnt)
				logrus.Infoln("******************")

				if len(sqls) > 0 {
					logrus.Infoln("Remediation SQL")
					for _, sql := range sqls {
						fmt.Printf("%s;\n", sql)
					}
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
	analyzeFkOrphanCmd.Flags().BoolVarP(&sqlFlag, "sql", "s", false, "Output SQL to remediate")

}
