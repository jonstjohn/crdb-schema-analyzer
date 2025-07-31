package cmd

import (
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/analyze"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var primaryRegionFlag string
var writeToFileFlag bool

var convertRbr2rbtCmd = &cobra.Command{
	Use:   "rbr2rbt",
	Short: "Convert RBR to RBT tables",
	Long:  "Convert regional by row to regional by table in the primary region",
	RunE: func(cmd *cobra.Command, args []string) error {

		converter, err := analyze.NewConverter(analyze.ConverterConfig{
			DbUrl:    urlFlag,
			Database: databaseFlag,
		})

		if err != nil {
			return err
		}

		statements, err := converter.Rbr2rbtSqlStatements(primaryRegionFlag)
		if err != nil {
			return err
		}

		if writeToFileFlag {

			// Create directory
			dir := "tmp"
			err := os.MkdirAll(dir, 0755)
			if err != nil {
				fmt.Println("Error creating directory:", err)
				return err
			}

			defaultFilePath := filepath.Join(dir, "default.sql")
			defaultFile, err := os.Create(defaultFilePath)
			defer defaultFile.Close()
			fileStartRe := regexp.MustCompile(`^-- FILE START (.*)$`)

			activeFile := defaultFile
			for _, statement := range statements {
				// If starting a file, create it and mark it as the active file
				if match := fileStartRe.FindStringSubmatch(statement); len(match) > 1 {
					fileName := match[1]
					filePath := filepath.Join(dir, fileName)
					file, err := os.Create(filePath)
					if err != nil {
						return err
					}
					activeFile = file
				} else if statement == "-- FILE END" {
					activeFile.Close()
					activeFile = defaultFile
				} else if len(statement) == 0 || strings.HasPrefix(statement, "--") { // comment
					_, err2 := activeFile.WriteString(fmt.Sprintf("%s\n", statement))
					if err2 != nil {
						return err2
					}
				} else {
					_, err2 := activeFile.WriteString(fmt.Sprintf("%s;\n", statement))
					if err2 != nil {
						return err2
					}
				}
			}

		} else {
			for _, statement := range statements {
				fmt.Printf("%s;\n", statement)
			}
		}

		return nil
	},
}

func init() {
	convertCmd.AddCommand(convertRbr2rbtCmd)
	convertRbr2rbtCmd.Flags().BoolVarP(&writeToFileFlag, "write-to-file", "f", false, "Write to file instead of stdout")
	convertRbr2rbtCmd.Flags().StringVarP(&primaryRegionFlag, "primary-region", "p", "", "primary region")
	err := convertRbr2rbtCmd.MarkFlagRequired("primary-region")
	if err != nil {
		panic(err)
	}
}
