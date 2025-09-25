package cmd

import (
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/spf13/cobra"
	"strings"
)

var analyzeRbrCommandRegionFlag string

var analyzeRbrDataCmd = &cobra.Command{
	Use:   "rbr_data",
	Short: "Analyze regional by row data",
	Long:  "Finds data outside of a specific region - TODO generalize.",
	RunE: func(cmd *cobra.Command, args []string) error {

		analyzer, err := analyze.NewAnalyzer(analyze.AnalyzerConfig{
			DbUrl:    urlFlag,
			Database: databaseFlag,
		})

		if err != nil {
			return err
		}

		localities, err := analyzer.GetRbrRowData(analyzeRbrCommandRegionFlag)

		if err != nil {
			return err
		}

		fmt.Printf("Tables with rows outside of %s\n", analyzeRbrCommandRegionFlag)
		for _, locality := range localities {
			if len(locality.RowLocality) > 0 {
				var strs []string
				for _, l := range locality.RowLocality {
					strs = append(strs, fmt.Sprintf("%s (%d)", l.Locality, l.Count))
				}
				fmt.Printf("%s: %s\n", locality.Table, strings.Join(strs, ", "))
			}
		}

		return nil
	},
}

func init() {
	analyzeCmd.AddCommand(analyzeRbrDataCmd)
	analyzeRbrDataCmd.PersistentFlags().StringVar(&analyzeRbrCommandRegionFlag, "region", "", "Region")
	analyzeRbrDataCmd.MarkPersistentFlagRequired("region")
}
