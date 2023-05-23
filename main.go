package main

import (
	"aggregator/app"
	"aggregator/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	config := utils.GetAppConfig()
	if config.Debug() {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	application := app.CreateApp(config)
	iterationsMade := application.Aggregate()
	log.Infof("Exiting, made %v iterations", iterationsMade)
}
