package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var analyzeFkRedundantCmd = &cobra.Command{
	Use:   "redundant",
	Short: "Analyze Potential FK orphans",
	RunE: func(cmd *cobra.Command, args []string) error {

		analyzer, err := analyze.NewAnalyzer(analyze.AnalyzerConfig{
			DbUrl:    urlFlag,
			Database: databaseFlag,
		})

		if err != nil {
			return err
		}

		redundants, err := analyzer.FKRedundants()
		if err != nil {
			return err
		}

		for _, redundant := range redundants {
			logrus.Infoln(redundant)
		}

		return nil
	},
}

func init() {
	analyzeFkCmd.AddCommand(analyzeFkRedundantCmd)
}
