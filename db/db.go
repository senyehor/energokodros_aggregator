package db

import (
	"aggregator/utils"
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type DB struct {
	conn *pgx.Conn
}

func GetDB() *DB {
	return &DB{conn: getConnection(composeConnectionString(utils.GetDBConfig()))}
}

func (db *DB) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return db.conn.Query(ctx, sql, args...)
}
func (db *DB) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return db.conn.QueryRow(ctx, sql, args...)
}

func (db *DB) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return db.conn.Exec(ctx, sql, arguments...)
}

func (db *DB) Begin(ctx context.Context) (pgx.Tx, error) {
	return db.conn.Begin(ctx)
}
