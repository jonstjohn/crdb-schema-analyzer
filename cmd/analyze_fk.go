package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var tablesFlag []string
var constraintsFlag []string
var rulesFlag []string

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

		filter, err := analyze.NewFKFilter(tablesFlag, constraintsFlag, rulesFlag)
		if err != nil {
			return err
		}
		constraints, err := analyzer.Fks(filter)
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
	analyzeFkCmd.PersistentFlags().StringSliceVar(&tablesFlag, "tables", []string{}, "Limit to tables (comma-separated)")
	analyzeFkCmd.PersistentFlags().StringSliceVar(&constraintsFlag, "constraints", []string{}, "Limit to constraints (comma-separated)")
	analyzeFkCmd.PersistentFlags().StringSliceVar(&rulesFlag, "rules", []string{}, "Limit to rules, e.g., ON DELETE CASCADE (comma-separated)")
}
