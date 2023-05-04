package db

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"os"
)

func getConnection(connString string) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig(connString)
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

func getDBConfig() *dbConfig {
	username, found := os.LookupEnv("DB_USERNAME")
	if !found {
		log.Error("failed to get username from env")
		os.Exit(1)
	}
	password, found := os.LookupEnv("DB_PASSWORD")
	if !found {
		log.Error("failed to get password from env")
		os.Exit(1)
	}
	host, found := os.LookupEnv("DB_HOST")
	if !found {
		log.Error("failed to get username from env")
		os.Exit(1)
	}
	port, found := os.LookupEnv("DB_PORT")
	if !found {
		log.Error("failed to get username from env")
		os.Exit(1)
	}
	name, found := os.LookupEnv("DB_NAME")
	if !found {
		log.Error("failed to get username from env")
		os.Exit(1)
	}
	return &dbConfig{
		username: username,
		password: password,
		host:     host,
		port:     port,
		name:     name,
	}
}

func composeConnectionString(config *dbConfig) string {
	return "postgres://" +
		config.username + ":" +
		config.password + "@" +
		config.host + ":" +
		config.port +
		"/" + config.name
}
