package main

import (
	"aggregator/app"
	"aggregator/utils"
	log "github.com/sirupsen/logrus"
	"os"
)

func main() {
	config := utils.GetAppConfig()
	if config.Debug() {
		log.SetLevel(log.DebugLevel)
	}
	logFile, _ := os.Create("./log")
	log.SetOutput(logFile)
	application := app.CreateApp(config)
	iterationsMade := application.Aggregate()
	log.Infof("Exiting, made %v iterations", iterationsMade)
}
