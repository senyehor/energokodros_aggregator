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
	}
	application := app.CreateApp(config)
	// todo 60 % interval
	iterationsMade := application.Aggregate()
	log.Infof("Exiting, made %v iterations", iterationsMade)
}
