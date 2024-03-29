package app

import (
	"aggregator/data_models"
	"aggregator/db"
	"aggregator/utils"
	"context"
	"errors"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
)

type App struct {
	connection                 *db.DB
	aggregationIntervalSeconds int64
}

func CreateApp(config *utils.AppConfig) *App {
	return &App{connection: db.GetDB(), aggregationIntervalSeconds: config.AggregationIntervalMinutes() * 60}
}

func (a *App) Aggregate() int {
	iterationsCount := 0
	latestRecords, found := a.getLatestRecordsDateInDescending()
	for found {
		a.checkRecordOrderedProperly(latestRecords)
		a.aggregate(latestRecords)
		latestRecords, found = a.getLatestRecordsDateInDescending()
		iterationsCount++
	}
	a.VacuumSensorRecordsTable()
	return iterationsCount
}

func (a *App) aggregate(latestRecords []*data_models.SensorValueRecord) {
	aggregationPeriods := a.processRecords(latestRecords)
	tx := startTransaction(a.connection, context.Background())
	defer tx.Rollback(context.Background())
	a.updateAggregationTable(aggregationPeriods, tx)
	a.deleteProcessedRecords(latestRecords, tx)
	commitTransaction(tx)
}

func (a *App) processRecords(latestRecords []*data_models.SensorValueRecord) *data_models.AggregationPeriodsStorage {
	earliestRecordTimeInsertedTruncatedToHoursUnix := a.getEarliestRecordInsertedTimeTruncatedToHoursUnix(latestRecords)
	aggregationPeriods := data_models.NewAggregationPeriodsStorage()
	for _, record := range latestRecords {
		a.createAccumulationPeriodsForRecordAndDistributeConsumptionBetweenThem(
			record,
			aggregationPeriods,
			earliestRecordTimeInsertedTruncatedToHoursUnix,
		)
	}
	return aggregationPeriods
}

func (a *App) createAccumulationPeriodsForRecordAndDistributeConsumptionBetweenThem(
	record *data_models.SensorValueRecord,
	aggregationPeriods *data_models.AggregationPeriodsStorage,
	earliestRecordTimeInTruncatedUnix int64,
) {
	accumulationPeriod, tooShort := data_models.NewAccumulationPeriod(record)
	if tooShort {
		return
	}
	aggregationPeriodData := data_models.NewAggregationPeriodData(
		record.BoxesSetID,
		earliestRecordTimeInTruncatedUnix,
		a.aggregationIntervalSeconds,
	)
	a.createAggregationPeriodsForAggregatedRecordData(
		aggregationPeriods,
		aggregationPeriodData,
		accumulationPeriod,
	)
	a.distributeRecordWholeConsumptionBetweenAggregationIntervals(
		accumulationPeriod,
		aggregationPeriodData,
		aggregationPeriods,
	)
}

func (a *App) updateAggregationTable(storage *data_models.AggregationPeriodsStorage, tx pgx.Tx) {
	iterator := storage.Iter()
	for iterator.HasNext() {
		aggregationPeriod := iterator.GetAggregationPeriod()
		sensorValueId, found := getCorrespondingIDForAggregationPeriod(
			a.connection,
			context.Background(),
			aggregationPeriod,
		)
		if found {
			updateAggregationTable(
				tx,
				context.Background(),
				sensorValueId,
				aggregationPeriod.SensorValues,
			)
			continue
		}
		insertIntoAggregationTable(
			tx,
			context.Background(),
			aggregationPeriod,
		)
	}
}

func (a *App) deleteProcessedRecords(records []*data_models.SensorValueRecord, tx pgx.Tx) {
	deleteProcessedSensorValuesRecords(tx, context.Background(), records)
}

func (a *App) createAggregationPeriodsForAggregatedRecordData(
	aggregatedPeriods *data_models.AggregationPeriodsStorage,
	aggregationPeriodData *data_models.AggregationPeriodData,
	accumulationPeriod *data_models.AccumulationPeriod) {
	aggregatedPeriods.CreatePeriodIfNotExists(aggregationPeriodData)
	for accumulationPeriod.EndUnix > aggregationPeriodData.EndUnix {
		aggregationPeriodData.EndUnix += aggregationPeriodData.AggregationIntervalSeconds
		aggregationPeriodData.StartUnix += aggregationPeriodData.AggregationIntervalSeconds
		aggregatedPeriods.CreatePeriodIfNotExists(aggregationPeriodData)
	}
}

func (a *App) distributeRecordWholeConsumptionBetweenAggregationIntervals(
	accumulationPeriod *data_models.AccumulationPeriod,
	aggregationPeriodData *data_models.AggregationPeriodData,
	aggregatedPeriods *data_models.AggregationPeriodsStorage) {

	for accumulationPeriod.EndUnix > accumulationPeriod.StartUnix {
		if a.checkRecordAccumulationPeriodCompletelyInAggregationInterval(aggregationPeriodData, accumulationPeriod) {

			consumedDuringInterval :=
				accumulationPeriod.AverageConsumption * (float64)(accumulationPeriod.DurationSeconds)
			aggregatedPeriods.AddSensorValueForRecord(aggregationPeriodData, consumedDuringInterval)
			break
		} else if a.checkRecordAccumulationPeriodEndInAggregationIntervalButStartIsOut(
			aggregationPeriodData, accumulationPeriod) {

			consumedDuringInterval := accumulationPeriod.AverageConsumption *
				(float64)(accumulationPeriod.EndUnix-aggregationPeriodData.StartUnix)
			aggregatedPeriods.AddSensorValueForRecord(aggregationPeriodData, consumedDuringInterval)
			// make adjustments to cover remaining part of accumulation period in further iteration
			accumulationPeriod.EndUnix = aggregationPeriodData.StartUnix
			aggregationPeriodData.EndUnix = aggregationPeriodData.StartUnix
			aggregationPeriodData.StartUnix =
				aggregationPeriodData.StartUnix - aggregationPeriodData.AggregationIntervalSeconds

			accumulationPeriod.DurationSeconds = accumulationPeriod.EndUnix - accumulationPeriod.StartUnix
		}
	}
}

func (a *App) checkRecordAccumulationPeriodCompletelyInAggregationInterval(
	aggregationInterval *data_models.AggregationPeriodData, accumulationPeriod *data_models.AccumulationPeriod) bool {

	return (accumulationPeriod.StartUnix >= aggregationInterval.StartUnix &&
		accumulationPeriod.StartUnix < aggregationInterval.EndUnix) &&
		(accumulationPeriod.EndUnix > aggregationInterval.StartUnix &&
			accumulationPeriod.EndUnix <= aggregationInterval.EndUnix)
}

func (a *App) checkRecordAccumulationPeriodEndInAggregationIntervalButStartIsOut(
	aggregationInterval *data_models.AggregationPeriodData, accumulationPeriod *data_models.AccumulationPeriod) bool {

	return (accumulationPeriod.StartUnix < aggregationInterval.StartUnix) &&
		(accumulationPeriod.EndUnix > aggregationInterval.StartUnix) &&
		(accumulationPeriod.EndUnix <= aggregationInterval.EndUnix)
}

func (a *App) getLatestRecordsDateInDescending() ([]*data_models.SensorValueRecord, bool) {
	latestRecords, err := getLatestRecords(a.connection, context.Background(), utils.GetAppConfig().QueryLimit())
	if err != nil {
		log.Error(err)
		panic(err)
	}
	if len(latestRecords) == 0 {
		return nil, false
	}
	return latestRecords, true
}

func (a *App) VacuumSensorRecordsTable() {
	vacuumSensorsRecords(a.connection, context.Background())
}

func (a *App) getEarliestRecordInsertedTimeTruncatedToHoursUnix(latestRecords []*data_models.SensorValueRecord) int64 {
	return truncateToHourUnix(latestRecords[len(latestRecords)-1].RecordInsertedTimeUnix, 0)
}

func (a *App) checkRecordOrderedProperly(latestRecords []*data_models.SensorValueRecord) {
	latestRecordTimeIn := latestRecords[0].RecordInsertedTimeUnix
	earliestRecordTimeIn := latestRecords[len(latestRecords)-1].RecordInsertedTimeUnix
	if latestRecordTimeIn < earliestRecordTimeIn {
		err := errors.New("records came in wrong order")
		log.Error(err)
		panic(err)
	}
}
