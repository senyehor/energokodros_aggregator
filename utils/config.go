package utils

import (
	"errors"
	log "github.com/sirupsen/logrus"
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
	sensorValuesQueryLimit     int64
}

func GetAppConfig() *AppConfig {
	debug := getBoolFromEnv("APP_DEBUG")
	aggregationIntervalMinutes := getInt64FromEnv("APP_AGGREGATION_INTERVAL_MINUTES")
	if !(aggregationIntervalMinutes == 15 || aggregationIntervalMinutes == 30 || aggregationIntervalMinutes == 60) {
		err := errors.New("aggregation interval should be 15, 30 or 60 minutes")
		log.Error(err)
		panic(err)
	}
	queryLimit := getInt64FromEnv("APP_QUERY_LIMIT")
	if queryLimit < 0 {
		err := errors.New("query limit cannot be below 0")
		panic(err)
	}
	return &AppConfig{
		debug:                      debug,
		aggregationIntervalMinutes: aggregationIntervalMinutes,
		sensorValuesQueryLimit:     queryLimit,
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
	return int(a.sensorValuesQueryLimit)
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
