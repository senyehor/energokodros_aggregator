package utils

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

func UnixToKievFormat(seconds, milliseconds int64) time.Time {
	kiyvFormat, _ := time.LoadLocation("Europe/Kiev")
	return time.Unix(seconds, milliseconds).In(kiyvFormat)
}

func ShortTimeFormat(t time.Time) string {
	return t.Format("01-02 15:04")
}

func getValueFromEnv(key string) string {
	value, found := os.LookupEnv(key)
	if !found {
		log.Errorf("failed to get %v from env", key)
		os.Exit(1)
	}
	return strings.Trim(value, "\"")
}

func getInt64FromEnv(key string) int64 {
	value := getValueFromEnv(key)
	res, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Errorf("failed to parse %v as int from env", value)
		os.Exit(1)
	}
	return res
}

func getBoolFromEnv(key string) bool {
	value := getValueFromEnv(key)
	res, err := strconv.ParseBool(value)
	if err != nil {
		log.Error("failed to parse %v as bool from env", value)
		os.Exit(1)
	}
	return res
}
