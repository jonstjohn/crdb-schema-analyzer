package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var analyzeFkRedundantCmd = &cobra.Command{
	Use:   "redundant",
	Short: "Analyze redundant FKs",
	Long: "Finds foreign key constraints that are redundant - i.e., are already contained in" +
		" another FK constraint. This primarily occurs with regional by row tables, where an FK exists that includes" +
		" crdb_region and one that does not. This is typically used when FKs implement ON DELETE CASCADE SET NULL" +
		" which is not supported for FKs using crdb_region. This narrow case is the focus of the current " +
		" redundant detection.",
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
