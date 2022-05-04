package db

import (
	"github.com/jackc/pgx/v4/pgxpool"
)

type DB struct {
	conn *pgxpool.Pool
}

type dbConfig struct {
	username,
	password,
	host,
	port,
	name string
}

func GetDB() *DB {
	return &DB{conn: getConnection(composeConnectionString(getDBConfig()))}
}
