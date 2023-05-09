package data_models

import (
	"aggregator/utils"
	"fmt"
	"time"
)

type SensorValueRecord struct {
	Id                                  int
	BoxesSetID                          int
	RecordDateUnix                      int64
	ValueAccumulationPeriodMilliseconds int64
	SensorValue                         float64
	PacketID                            int
}

type AggregationPeriod struct {
	Data         *AggregationPeriodData
	SensorValues float64
}

type AccumulationPeriod struct {
	DurationSeconds,
	StartUnix,
	EndUnix int64
	AverageConsumption float64
}

type AggregationPeriodData struct {
	BoxesSetID int
	StartUnix  int64
	EndUnix    int64
}

type AggregationPeriodsStorage struct {
	Storage []*AggregationPeriod
}

func (a *AggregationPeriodsStorage) GetIndexForPeriodOrCreate(aggregationPeriodData *AggregationPeriodData) int {
	for index, record := range a.Storage {
		if a.checkRecordMatches(record, aggregationPeriodData) {
			return index
		}
	}
	a.Storage = append(a.Storage, newAggregationPeriod(aggregationPeriodData, 0))
	return len(a.Storage) - 1
}

func (a *AggregationPeriodsStorage) checkRecordMatches(
	record *AggregationPeriod, aggregationPeriodData *AggregationPeriodData) bool {

	return record.Data.StartUnix == aggregationPeriodData.StartUnix &&
		record.Data.EndUnix == aggregationPeriodData.EndUnix &&
		record.Data.BoxesSetID == aggregationPeriodData.BoxesSetID
}

func (a *AggregationPeriodsStorage) AddSensorValueForRecord(data *AggregationPeriodData, value float64) {
	a.Storage[a.GetIndexForPeriodOrCreate(data)].SensorValues += value
}

func (a *AggregationPeriodsStorage) DeleteEmptyPeriods() {
	length := len(a.Storage)
	for i := 0; i < length; i++ {
		if a.Storage[i].SensorValues == 0 {
			a.Storage = append(a.Storage[:i], a.Storage[i+1:]...)
			length--
			i--
		}
	}
}

func (a *AggregationPeriodData) Copy() *AggregationPeriodData {
	return &AggregationPeriodData{
		BoxesSetID: a.BoxesSetID,
		StartUnix:  a.StartUnix,
		EndUnix:    a.EndUnix,
	}
}

// NewAccumulationPeriod returns accumulation period and whether it was too short to create
func NewAccumulationPeriod(record *SensorValueRecord) (*AccumulationPeriod, bool) {
	accumulationPeriodSeconds := record.ValueAccumulationPeriodMilliseconds / 1000
	// we do not care about some milliseconds left
	if accumulationPeriodSeconds <= 0 {
		return nil, true
	}
	accumulationPeriodStart := record.RecordDateUnix - accumulationPeriodSeconds
	accumulationPeriodEnd := record.RecordDateUnix
	averageConsumption := record.SensorValue / (float64)(accumulationPeriodSeconds)

	return &AccumulationPeriod{
		DurationSeconds:    accumulationPeriodSeconds,
		StartUnix:          accumulationPeriodStart,
		EndUnix:            accumulationPeriodEnd,
		AverageConsumption: averageConsumption,
	}, false
}

func NewAggregationPeriodData(boxesSetID int, aggregationPeriodStartUnix int64,
	aggregationPeriodEndUnix int64) *AggregationPeriodData {

	return &AggregationPeriodData{
		BoxesSetID: boxesSetID,
		StartUnix:  aggregationPeriodStartUnix,
		EndUnix:    aggregationPeriodEndUnix,
	}
}

func NewAggregationPeriodsStorage() *AggregationPeriodsStorage {
	return &AggregationPeriodsStorage{Storage: []*AggregationPeriod{}}
}
func newAggregationPeriod(aggregationPeriodData *AggregationPeriodData, sensorValue float64) *AggregationPeriod {
	return &AggregationPeriod{
		Data:         aggregationPeriodData.Copy(),
		SensorValues: sensorValue,
	}
}

func (s *SensorValueRecord) Repr() string {
	accumulationPeriod, _ := time.ParseDuration(fmt.Sprintf("%v"+"ms", s.ValueAccumulationPeriodMilliseconds))
	return "record date is from " +
		utils.ShortTimeFormat(
			utils.UnixToKievFormat(s.RecordDateUnix-s.ValueAccumulationPeriodMilliseconds/1000,
				0)) +
		" to " + utils.ShortTimeFormat(utils.UnixToKievFormat(s.RecordDateUnix, 0)) +
		fmt.Sprintf(" and it (%v) was accumulated during ", s.SensorValue) + accumulationPeriod.String()
}
func (a *AggregationPeriod) Repr() string {
	return a.Data.Repr() + fmt.Sprintf(" sensors values %v", a.SensorValues)
}
func (a *AccumulationPeriod) Repr() string {
	duration, _ := time.ParseDuration(fmt.Sprintf("%v"+"s", a.DurationSeconds))
	return fmt.Sprintf(
		"duration is %v start %v end %v avg consumption %v",
		duration,
		utils.ShortTimeFormat(utils.UnixToKievFormat(a.StartUnix, 0)),
		utils.ShortTimeFormat(utils.UnixToKievFormat(a.EndUnix, 0)),
		a.AverageConsumption,
	)
}
func (a *AggregationPeriodData) Repr() string {
	return fmt.Sprintf("boxes set id %v start %v end %v",
		a.BoxesSetID,
		utils.ShortTimeFormat(utils.UnixToKievFormat(a.StartUnix, 0)),
		utils.ShortTimeFormat(utils.UnixToKievFormat(a.EndUnix, 0)),
	)
}
