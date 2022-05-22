package db

import (
	"aggregator/utils"
	"context"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"os"
)

func getConnection(connString string) *pgx.Conn {
	config, err := pgx.ParseConfig(connString)
	if err != nil {
		log.Error(err)
		log.Error("Could not parse config")
		os.Exit(1)
	}

	pool, err := pgx.ConnectConfig(context.Background(), config)
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
