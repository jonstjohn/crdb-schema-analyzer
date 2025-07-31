package analyze

import (
	"bufio"
	"context"
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
)

type Executor struct {
	Config ExecutorConfig
	Db     *db.Db
}

type ExecutorConfig struct {
	DbUrl         string
	Database      string
	Concurrency   int
	PreSql        string
	UntilZeroRows bool
}

func NewExecutor(config ExecutorConfig) (*Executor, error) {
	d, err := db.NewDbDatasource(config.DbUrl, config.Database, false)
	if err != nil {
		return nil, err
	}
	return &Executor{
		Config: config,
		Db:     d,
	}, nil
}

// ExecuteFromFile executes statements from a file
// It looks for "-- BEGIN BLOCK" and "-- END BLOCK" and sends those as a batch to one of the goroutines
// If a statement occurs outside of a begin and end, it just sends the single statement
func (e *Executor) ExecuteFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var statementBlocks [][]string
	var currentBlock []string
	var inBlock bool

	// var lines []string
	for scanner.Scan() {
		// lines = append(lines, scanner.Text())
		line := strings.TrimSpace(scanner.Text())

		switch line {
		case "-- BEGIN BLOCK":
			if inBlock {
				return fmt.Errorf("nested -- BEGIN BLOCK not allowed")
			}
			inBlock = true
			currentBlock = []string{}

		case "-- END BLOCK":
			if !inBlock {
				return fmt.Errorf("-- END BLOCK without -- BEGIN BLOCK")
			}
			inBlock = false
			if len(currentBlock) > 0 {
				statementBlocks = append(statementBlocks, currentBlock)
			}

		default:
			if line == "" || strings.HasPrefix(line, "--") { // empty line or comment
				continue
			}
			if inBlock {
				currentBlock = append(currentBlock, line)
			} else {
				statementBlocks = append(statementBlocks, []string{line})
			}
		}
	}
	if scanner.Err() != nil {
		return scanner.Err()
	}
	if inBlock {
		return fmt.Errorf("unclosed -- BEGIN BLOCK at end of file")
	}

	sqlChan := make(chan []string, len(statementBlocks))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < e.Config.Concurrency; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for batch := range sqlChan {
				logrus.Infof("Executing [%d]: %s\n", workerId, strings.Join(batch, "\n"))
				if err := e.executeSQLStatements(batch); err != nil {
					logrus.Errorf("Error [%d]: %v", workerId, err)
				}
			}
		}(i)
	}

	// Send SQL statements to workers
	for _, block := range statementBlocks {
		sqlChan <- block
	}
	close(sqlChan)

	// Wait for all workers to finish
	wg.Wait()
	return nil

}

func (e *Executor) executeSQLStatements(statements []string) error {
	conn, err := e.Db.Pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()
	if e.Config.PreSql != "" {
		_, err = conn.Exec(context.Background(), e.Config.PreSql)
		if err != nil {
			return fmt.Errorf("error executing pre-sql [%s]: %w", e.Config.PreSql, err)
		}
	}

	// Execute SQL statements - once unless UntilZeroRows is set to true, in which case we are performing a batch
	// operation and want to continue
	keepGoing := true
	for i, statement := range statements {
		for keepGoing {
			tag, err := conn.Exec(context.Background(), statement)
			if err != nil {
				return fmt.Errorf("failed on statement %d '%s': %w", i+1, statement, err)
			}
			keepGoing = e.Config.UntilZeroRows && tag.RowsAffected() > 0
		}
	}
	return nil
}
