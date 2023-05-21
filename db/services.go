package db

import (
	"aggregator/utils"
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"os"
)

func getConnection(connString string) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig(connString)
	config.MaxConns = 64
	if err != nil {
		log.Error(err)
		log.Error("Could not parse config")
		os.Exit(1)
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	return pool
}

func composeConnectionString(config *utils.DBConfig) string {
	return "postgres://" +
		config.Username() + ":" +
		config.Password() + "@" +
		config.Host() + ":" +
		config.Port() +
		"/" + config.Name()
}
