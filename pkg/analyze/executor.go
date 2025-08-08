package analyze

import (
	"context"
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
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
	d, err := db.NewDbDatasource(config.DbUrl, config.Database, false, config.Concurrency)
	if err != nil {
		return nil, err
	}
	return &Executor{
		Config: config,
		Db:     d,
	}, nil
}

func (e *Executor) ExecuteFromFile(filePath string) error {

	parser := NewSqlFileParser(filePath)
	statementBlocks, err := parser.Parse()
	if err != nil {
		return err
	}

	sqlChan := make(chan []string, len(statementBlocks))
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < e.Config.Concurrency; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for batch := range sqlChan {
				logrus.Infof("Executing [%d]:\n%s", workerId, strings.Join(batch, "\n---\n"))
				if err := e.executeSQLStatements(batch); err != nil {
					logrus.Errorf("Error [%d]: %v", workerId, err)
				}
			}
		}(i)
	}

	for _, block := range statementBlocks {
		sqlChan <- block
	}
	close(sqlChan)

	wg.Wait()
	logrus.Infof("Completed executing all SQL in %s", time.Since(start))
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

	// Execute SQL statements
	for i, statement := range statements {
		// Execute once unless UntilZeroRows is set to true, in which case we are performing a batch
		// operation and want to continue
		keepGoing := true
		for keepGoing {
			start := time.Now()
			tag, err := conn.Exec(context.Background(), statement)
			if err != nil {
				return fmt.Errorf("failed on statement %d '%s': %w", i+1, statement, err)
			}
			logrus.Infof("Successfully executed '%s' in %s", statement, time.Since(start))
			keepGoing = e.Config.UntilZeroRows && tag.RowsAffected() > 0
		}
	}
	return nil
}
