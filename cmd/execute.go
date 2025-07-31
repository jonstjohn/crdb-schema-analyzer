package cmd

import "github.com/spf13/cobra"

var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "Execute statements",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(executeCmd)
}
