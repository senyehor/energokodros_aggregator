package utils

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

func UnixToKievTZ(seconds, milliseconds int64) time.Time {
	kiyvFormat, _ := time.LoadLocation("Europe/Kiev")
	return time.Unix(seconds, milliseconds).In(kiyvFormat)
}

func ShortTimeFormat(t time.Time) string {
	return t.Format("01-02 15:04.05")
}

func getValueFromEnv(key string) string {
	value, found := os.LookupEnv(key)
	if !found {
		err := fmt.Errorf("failed to get %v from env", key)
		log.Error(err)
		panic(err)
	}
	return strings.Trim(value, "\"")
}

func getInt64FromEnv(key string) int64 {
	value := getValueFromEnv(key)
	res, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		err := fmt.Errorf("failed to parse %v as int from env", value)
		panic(err)
	}
	return res
}

func getBoolFromEnv(key string) bool {
	value := getValueFromEnv(key)
	res, err := strconv.ParseBool(value)
	if err != nil {
		err := fmt.Errorf("failed to parse %v as bool from env", value)
		panic(err)
	}
	return res
}
