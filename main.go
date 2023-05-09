package main

import (
	"aggregator/app"
	"aggregator/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	application := app.CreateApp()
	config := utils.GetAppConfig()
	if config.Debug() {
		log.SetLevel(log.DebugLevel)
	}
	iterationsMade := application.Aggregate(config.AggregationIntervalMinutes() * 60)
	log.Infof("Exiting, made %v iterations", iterationsMade)
}
