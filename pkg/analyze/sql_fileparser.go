package analyze

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type SqlFileParser struct {
	Path string
}

func NewSqlFileParser(path string) *SqlFileParser {
	return &SqlFileParser{Path: path}
}

func (s *SqlFileParser) Parse() ([][]string, error) {

	var statementBlocks [][]string
	file, err := os.Open(s.Path)
	if err != nil {
		return statementBlocks, err
	}
	defer file.Close()

	return s.ParseReader(file)
}

// ParseReader parses a io.Reader into batches of statements.
// Each batch is a slice of complete SQL statements (terminated by ";").
// Blocks between -- BEGIN BLOCK and -- END BLOCK are grouped together as one batch.
func (s *SqlFileParser) ParseReader(r io.Reader) ([][]string, error) {
	scanner := bufio.NewScanner(r)

	var statementBlocks [][]string
	var currentBlock []string
	var inBlock bool
	var inDollarQuote bool
	var currentStmtLines []string

	flushStatement := func() {
		stmt := strings.TrimSpace(strings.Join(currentStmtLines, "\n"))
		if stmt != "" {
			if inBlock {
				currentBlock = append(currentBlock, stmt)
			} else {
				statementBlocks = append(statementBlocks, []string{stmt})
			}
		}
		currentStmtLines = nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		switch trimmed {
		case "-- BEGIN BLOCK":
			if inBlock {
				return nil, fmt.Errorf("nested -- BEGIN BLOCK not allowed")
			}
			inBlock = true
			currentBlock = []string{}
			currentStmtLines = nil
			continue

		case "-- END BLOCK":
			if !inBlock {
				return nil, fmt.Errorf("-- END BLOCK without -- BEGIN BLOCK")
			}
			flushStatement()
			inBlock = false
			if len(currentBlock) > 0 {
				statementBlocks = append(statementBlocks, currentBlock)
			}
			continue
		}

		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Toggle in/out of dollar-quoted function body
		// Very basic check: line contains $$ and it's not inside a quoted string
		if strings.Count(line, "$$")%2 != 0 {
			inDollarQuote = !inDollarQuote
		}

		currentStmtLines = append(currentStmtLines, line)

		if !inDollarQuote && strings.HasSuffix(trimmed, ";") {
			flushStatement()
		}
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}
	if inBlock {
		return nil, fmt.Errorf("unclosed -- BEGIN BLOCK at end of file")
	}
	if inDollarQuote {
		return nil, fmt.Errorf("unclosed $$ dollar-quote block at end of file")
	}

	return statementBlocks, nil
}
