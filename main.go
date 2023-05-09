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
	iterationsCount := 0
	for application.Aggregate(config.AggregationIntervalMinutes() * 60) {
		iterationsCount++
	}
	log.Infof("Successfully aggregated, made %v", iterationsCount)
	application.Vacuum()
}
