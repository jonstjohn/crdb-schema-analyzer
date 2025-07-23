package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var analyzeFkCmd = &cobra.Command{
	Use:   "fk",
	Short: "Analyze FK constraints",
	RunE: func(cmd *cobra.Command, args []string) error {

		analyzer, err := analyze.NewAnalyzer(analyze.AnalyzerConfig{
			DbUrl:    urlFlag,
			Database: databaseFlag,
		})

		if err != nil {
			return err
		}

		constraints, err := analyzer.Fks()
		for _, constraint := range constraints {
			logrus.Infoln(constraint)
		}
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	analyzeCmd.AddCommand(analyzeFkCmd)
}
