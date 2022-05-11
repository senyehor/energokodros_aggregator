package app

import (
	"aggregator/data_models"
	"aggregator/db"
	"aggregator/utils"
	"context"
	log "github.com/sirupsen/logrus"
	"os"
)

type App struct {
	connection *db.DB
}

func CreateApp() *App {
	return &App{connection: db.GetDB()}
}

func (a *App) Aggregate(aggregationIntervalSeconds int64) int {
	iterationsCount := 0
	latestRecords, found := a.getLatestRecordsDateInDescending()
	for found {
		a.checkRecordOrderedProperly(latestRecords)
		a.aggregate(aggregationIntervalSeconds, latestRecords)
		latestRecords, found = a.getLatestRecordsDateInDescending()
		iterationsCount++
	}
	a.VacuumSensorRecordsTable()
	return iterationsCount
}

func (a *App) checkRecordOrderedProperly(latestRecords []*data_models.SensorValueRecord) {
	latestRecordTimeIn := latestRecords[0].RecordInsertedTimeUnix
	earliestRecordTimeIn := latestRecords[len(latestRecords)-1].RecordInsertedTimeUnix
	if latestRecordTimeIn < earliestRecordTimeIn {
		log.Error("records came in wrong order")
		os.Exit(1)
	}
}

func (a *App) aggregate(aggregationIntervalSeconds int64, latestRecords []*data_models.SensorValueRecord) bool {
	aggregationPeriods := a.processRecords(aggregationIntervalSeconds, latestRecords)
	a.updateAggregationTable(aggregationPeriods)
	a.deleteProcessedRecords(latestRecords)
	return true
}

func (a *App) processRecords(aggregationIntervalSeconds int64,
	latestRecords []*data_models.SensorValueRecord) *data_models.AggregationPeriodsStorage {

	earliestRecordTimeInsertedTruncatedToHoursUnix := a.getEarliestRecordInsertedTimeTruncatedToHoursUnix(latestRecords)
	aggregationPeriods := data_models.NewAggregationPeriodsStorage()
	for _, record := range latestRecords {
		a.createAccumulationPeriodsForRecordAndDistributeConsumptionBetweenThem(
			record,
			aggregationPeriods,
			earliestRecordTimeInsertedTruncatedToHoursUnix,
			aggregationIntervalSeconds,
		)
	}
	aggregationPeriods.DeleteEmptyPeriods()
	return aggregationPeriods
}

func (a *App) createAccumulationPeriodsForRecordAndDistributeConsumptionBetweenThem(
	record *data_models.SensorValueRecord,
	aggregationPeriods *data_models.AggregationPeriodsStorage,
	earliestRecordTimeInTruncatedUnix,
	aggregationIntervalSeconds int64,
) {
	accumulationPeriod, tooShort := data_models.NewAccumulationPeriod(record)
	if tooShort {
		return
	}
	aggregationPeriodData := data_models.NewAggregationPeriodData(
		record.BoxesSetID,
		earliestRecordTimeInTruncatedUnix-aggregationIntervalSeconds,
		earliestRecordTimeInTruncatedUnix,
	)
	a.createAggregationPeriodsForAggregatedRecordData(
		aggregationPeriods,
		aggregationPeriodData,
		accumulationPeriod,
		aggregationIntervalSeconds,
	)
	a.distributeRecordWholeConsumptionBetweenAggregationIntervals(
		accumulationPeriod,
		aggregationPeriodData,
		aggregationPeriods,
		aggregationIntervalSeconds,
	)
}

func (a *App) updateAggregationTable(storage *data_models.AggregationPeriodsStorage) {
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
				a.connection,
				context.Background(),
				sensorValueId,
				aggregationPeriod.SensorValues,
			)
		} else {
			insertIntoAggregationTable(
				a.connection,
				context.Background(),
				aggregationPeriod,
			)
		}
	}
}

func (a *App) deleteProcessedRecords(records []*data_models.SensorValueRecord) {
	deleteProcessedSensorValuesRecords(a.connection, context.Background(), records)
}

func (a *App) createAggregationPeriodsForAggregatedRecordData(
	aggregatedPeriods *data_models.AggregationPeriodsStorage,
	aggregationPeriodData *data_models.AggregationPeriodData,
	accumulationPeriod *data_models.AccumulationPeriod,
	aggregationIntervalSeconds int64,
) {

	aggregatedPeriods.CreatePeriodIfNotExists(aggregationPeriodData)

	for accumulationPeriod.EndUnix > aggregationPeriodData.EndUnix {
		aggregationPeriodData.EndUnix += aggregationIntervalSeconds
		aggregationPeriodData.StartUnix += aggregationIntervalSeconds
		aggregatedPeriods.CreatePeriodIfNotExists(aggregationPeriodData)
	}
}

func (a *App) distributeRecordWholeConsumptionBetweenAggregationIntervals(
	accumulationPeriod *data_models.AccumulationPeriod,
	aggregationPeriodData *data_models.AggregationPeriodData,
	aggregatedPeriods *data_models.AggregationPeriodsStorage,
	aggregationIntervalSeconds int64) {

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
				aggregationPeriodData.StartUnix - aggregationIntervalSeconds

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
		os.Exit(1)
	}
	if len(latestRecords) == 0 {
		log.Infof("No records in db, finishing")
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
