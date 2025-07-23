package cmd

import "github.com/spf13/cobra"

var analyzeRefreshCacheFlag bool
var analyzeDatabaseFlag string

var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze statement/transaction statistics and contention events",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
}
