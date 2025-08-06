package dbpgx

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPoolFromUrl provided a pgxpool.Pool instance using the connection string
// It also takes a maxConnections parameter. Although this can be specified by pool_max_connections in the URL
// some of the calling code uses a concurrency that requires maxConnections
func NewPoolFromUrl(url string, maxConnections int) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	// Use the higher of maxConnections and config.MaxConns
	if int32(maxConnections) > config.MaxConns {
		config.MaxConns = int32(maxConnections)
	}
	return pgxpool.NewWithConfig(context.Background(), config)
}
