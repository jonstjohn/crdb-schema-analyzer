package cmd

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/spf13/cobra"
)

var concurrencyFlag int
var fileFlag string
var preSqlFlag string
var untilZeroRowsFlag bool

var executeParallelCmd = &cobra.Command{
	Use:   "parallel",
	Short: "Execute SQL commands in parallel",
	Long:  "Executes SQL command in parallel from a file",
	RunE: func(cmd *cobra.Command, args []string) error {

		executor, err := analyze.NewExecutor(analyze.ExecutorConfig{
			DbUrl:         urlFlag,
			Database:      databaseFlag,
			Concurrency:   concurrencyFlag,
			PreSql:        preSqlFlag,
			UntilZeroRows: untilZeroRowsFlag,
		})

		if err != nil {
			return err
		}

		err = executor.ExecuteFromFile(fileFlag)

		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	executeCmd.AddCommand(executeParallelCmd)
	executeParallelCmd.Flags().IntVarP(&concurrencyFlag, "concurrency", "c", 5, "Number of concurrent queries")
	executeParallelCmd.Flags().StringVarP(&fileFlag, "file", "f", "", "File containing SQL statements")
	executeParallelCmd.Flags().StringVarP(&preSqlFlag, "pre-sql", "p", "", "Pre-SQL statement - runs before each statement")
	executeParallelCmd.Flags().BoolVarP(&untilZeroRowsFlag, "until-zero-rows", "u", false, "Run SQL statements until zero rows are returned")
	err := executeParallelCmd.MarkFlagRequired("file")
	if err != nil {
		panic(err)
	}
}
