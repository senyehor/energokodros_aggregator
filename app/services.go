package app

import (
	"aggregator/data_models"
	"context"
	"errors"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
)

func cutOffEverythingFromHours(timeToCut time.Time) time.Time {
	return timeToCut.Round(time.Hour)
}

func getNewestRecords(conn *pgxpool.Pool, ctx context.Context, limit int) ([]data_models.SensorValueRecord, error) {
	selectLastRecordsQuery :=
		`SELECT
			sensor_value_id, boxes_set_id, value_datein, value_accumulation_period, sensor_value, package_number
		 FROM public.sensor_values
         ORDER BY sensor_value_id DESC LIMIT $1;`
	rows, err := conn.Query(ctx, selectLastRecordsQuery, limit)
	if err != nil {
		return nil, errors.New("failed to get newest records")
	}
	result, err := parseRows(rows, limit)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func parseRows(rows pgx.Rows, count int) ([]data_models.SensorValueRecord, error) {
	var timePGFormat pgtype.Timestamp
	var record data_models.SensorValueRecord
	result := make([]data_models.SensorValueRecord, count)

	for rows.Next() {
		err := rows.Scan(&record.Id, &record.BoxesSetID, &timePGFormat,
			&record.ValueAccumulationPeriodSeconds, &record.SensorValue, &record.PacketID)
		if err != nil {
			return nil, errors.New("something went wrong during scanning rows")
		}

		record.RecordDateUnix = timePGFormat.Time.Unix()
		result = append(result, record)
	}
	return result, nil
}
