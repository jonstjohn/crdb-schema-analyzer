package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var includeSizeFlag bool
var includeFKsFlag bool

var analyzeTablesCmd = &cobra.Command{
	Use:   "tables",
	Short: "Analyze all tables",
	RunE: func(cmd *cobra.Command, args []string) error {

		analyzer, err := analyze.NewAnalyzer(analyze.AnalyzerConfig{
			DbUrl:    urlFlag,
			Database: databaseFlag,
		})

		if err != nil {
			return err
		}

		tables, err := analyzer.Tables(includeSizeFlag, includeFKsFlag)
		for _, table := range tables {
			logrus.Infoln(table)
		}
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	analyzeCmd.AddCommand(analyzeTablesCmd)
	analyzeTablesCmd.Flags().BoolVarP(&includeSizeFlag, "include-size", "s", false, "Include table sizes (slower)")
	analyzeTablesCmd.Flags().BoolVarP(&includeFKsFlag, "include-foreign-keys", "f", false, "Include foreign keys (slower)")
}
