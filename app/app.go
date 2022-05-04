package app

import "aggregator/db"

type App struct {
	connection *db.DB
}

func CreateApp() *App {
	return &App{connection: db.GetDB()}
}

func (a *App) Aggregate(aggregationIntervalSeconds int64) {

}
