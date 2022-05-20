package app

import (
	"aggregator/data_models"
	"context"
	"errors"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func getLatestRecords(conn Connection, ctx context.Context, limit int) ([]*data_models.SensorValueRecord, error) {
	selectLastRecordsQuery :=
		`SELECT
			sensor_value_id, boxes_set_id, value_date_in, value_accumulation_period, sensor_value, package_number
         FROM public.sensor_values
	     ORDER BY sensor_value_id DESC LIMIT $1;`
	rows, err := conn.Query(ctx, selectLastRecordsQuery, limit)
	if err != nil {
		return nil, errors.New("failed to get newest records")
	}
	result, err := parseRowsFromSensorValues(rows, limit)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// getCorrespondingIDForAggregationPeriod returns sensor value id and
// whether there is a record corresponding to provided aggregationPeriod
func getCorrespondingIDForAggregationPeriod(
	conn Connection,
	ctx context.Context,
	aggregationPeriod *data_models.AggregationPeriod) (int, bool) {

	query :=
		`SELECT
			sensor_value_id
		FROM sensor_values_h
		WHERE 
			boxes_set_id=$1
			and aggregation_interval_start=$2
			and aggregation_interval_end=$3
			order by aggregation_interval_end desc limit 1;`

	periodStart := aggregationPeriod.Data.GetStartTime()
	periodEnd := aggregationPeriod.Data.GetEndTime()

	row := conn.QueryRow(ctx, query, aggregationPeriod.Data.BoxesSetID, periodStart, periodEnd)
	var sensorValueId int

	err := row.Scan(&sensorValueId)
	if err == pgx.ErrNoRows {
		return 0, false
	}
	if err != nil {
		log.Error("error occurred while getting sensor_values_h record")
		log.Debug(err)
		os.Exit(1)
	}
	return sensorValueId, true
}

func deleteProcessedSensorValuesRecords(conn Connection, ctx context.Context, records []*data_models.SensorValueRecord) {
	query := `DELETE FROM sensor_values where sensor_value_id between $1 and $2;`
	_, err := conn.Exec(ctx, query, records[len(records)-1].Id, records[0].Id)
	if err != nil {
		log.Error("error occurred while deleting aggregated records")
		log.Debug(err)
		os.Exit(1)
	}
}

func insertIntoAggregationTable(conn Connection, ctx context.Context, period *data_models.AggregationPeriod) {
	query :=
		`INSERT INTO sensor_values_h
         (boxes_set_id, aggregation_interval_start, aggregation_interval_end, sensor_value)
		 values ($1, $2, $3, $4)`
	_, err := conn.Exec(
		ctx,
		query,
		period.Data.BoxesSetID,
		period.Data.GetStartTime(),
		period.Data.GetEndTime(),
		period.SensorValues,
	)
	if err != nil {
		log.Error("error happened trying insert new aggregation period")
		log.Debug(err)
		os.Exit(1)
	}
}

func updateAggregationTable(conn Connection, ctx context.Context, sensorValueID int, sensorValueToAdd float64) {
	query := `UPDATE sensor_values_h set sensor_value=sensor_value + $2 where sensor_value_id=$1;`
	_, err := conn.Exec(ctx, query, sensorValueID, sensorValueToAdd)
	if err != nil {
		log.Error("error occurred while updating sensor_values_h record")
		log.Debug(err)
		os.Exit(1)
	}
}

func parseRowsFromSensorValues(rows Rows, maxRecordsCount int) ([]*data_models.SensorValueRecord, error) {
	result := make([]*data_models.SensorValueRecord, maxRecordsCount)

	var timePGFormat pgtype.Timestamptz
	var record *data_models.SensorValueRecord
	actualRecordsCount := 0
	for i := range result {
		if rows.Next() {
			record = &data_models.SensorValueRecord{}
			err := rows.Scan(&record.Id, &record.BoxesSetID, &timePGFormat,
				&record.ValueAccumulationPeriodMilliseconds, &record.SensorValue, &record.PacketID)
			if err != nil {
				return nil, errors.New("something went wrong during scanning rows")
			}
			record.RecordInsertedTimeUnix = timePGFormat.Time.Unix()
			result[i] = record
			actualRecordsCount++
		}
	}
	return result[:actualRecordsCount], nil
}

func vacuumSensorsRecords(conn Connection, ctx context.Context) {
	query := `vacuum (full) sensor_values;`
	_, err := conn.Exec(ctx, query)
	if err != nil {
		log.Error("error happened while vacuuming sensor_values")
		log.Debug(err)
		os.Exit(1)
	}
}

func truncateToHourUnix(seconds, milliseconds int64) int64 {
	return time.Unix(seconds, milliseconds).Truncate(time.Hour).Unix()
}

func logIntervalBeingProcessed(records []*data_models.SensorValueRecord) {
	latestRecord := records[0]
	earliestRecord := records[len(records)-1]
	log.Info(
		"processing interval from " +
			time.Unix(earliestRecord.RecordInsertedTimeUnix, 0).String() +
			" to " +
			time.Unix(latestRecord.RecordInsertedTimeUnix, 0).String(),
	)
}

func logCreatedIntervals(first, last *data_models.AggregationPeriod) {
	log.Infof("created intervals from %v to %v", first.Repr(), last.Repr())
}

func logCreatingAggregationPeriodsForAggregatedRecordData(aggregationPeriodData *data_models.AggregationPeriodData) {
	log.Infof("creating intervals for %v", aggregationPeriodData.Repr())
}
