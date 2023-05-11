package utils

import (
	log "github.com/sirupsen/logrus"
	"os"
)

type DBConfig struct {
	username,
	password,
	host,
	port,
	name string
}

type AppConfig struct {
	debug                      bool
	aggregationIntervalMinutes int64
	queryLimit                 int64
}

func GetAppConfig() *AppConfig {
	debug := getBoolFromEnv("APP_DEBUG")
	aggregationIntervalMinutes := getInt64FromEnv("APP_AGGREGATION_INTERVAL")
	if !(aggregationIntervalMinutes == 15 || aggregationIntervalMinutes == 30 || aggregationIntervalMinutes == 60) {
		log.Error("aggregation interval should be 15 30 or 60 minutes")
		os.Exit(1)
	}
	queryLimit := getInt64FromEnv("APP_QUERY_LIMIT")
	return &AppConfig{
		debug:                      debug,
		aggregationIntervalMinutes: aggregationIntervalMinutes,
		queryLimit:                 queryLimit,
	}
}
func GetDBConfig() *DBConfig {
	username := getValueFromEnv("DB_USERNAME")
	password := getValueFromEnv("DB_PASSWORD")
	host := getValueFromEnv("DB_HOST")
	port := getValueFromEnv("DB_PORT")
	name := getValueFromEnv("DB_NAME")
	return &DBConfig{
		username: username,
		password: password,
		host:     host,
		port:     port,
		name:     name,
	}
}

func (a AppConfig) QueryLimit() int {
	return int(a.queryLimit)
}
func (a AppConfig) Debug() bool {
	return a.debug
}
func (a AppConfig) AggregationIntervalMinutes() int64 {
	return a.aggregationIntervalMinutes
}

func (D DBConfig) Username() string {
	return D.username
}
func (D DBConfig) Password() string {
	return D.password
}
func (D DBConfig) Host() string {
	return D.host
}
func (D DBConfig) Port() string {
	return D.port
}
func (D DBConfig) Name() string {
	return D.name
}
