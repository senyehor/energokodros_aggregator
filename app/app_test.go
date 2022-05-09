package app

import (
	"aggregator/data_models"
	"aggregator/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"testing"
	"time"
)

const (
	second int64 = 1
	minute       = 60 * second
	hour         = 60 * minute
	day          = 24 * hour
	week         = 7 * day
)

func TestApp(t *testing.T) {
	suite.Run(t, new(appTestSuite))
}

type appTestSuite struct {
	suite.Suite
	r *require.Assertions
	averageConsumptionPerMillisecond,
	aggregationIntervalSeconds int64
	app *App
}

func (t *appTestSuite) SetupTest() {
	t.r = t.Require()
	// 400 - PC consumption Watt 23 - PC`s count in r class  9 * hour - working day length in seconds 7 - days in week
	t.averageConsumptionPerMillisecond = 400 * 23 / 1000
	t.aggregationIntervalSeconds = hour
	t.app = CreateApp()
}

func (t *appTestSuite) TestWeekAccumulationInterval() {
	rand.Seed(time.Now().Unix())

	weekInMilliseconds := week * 1000
	consumed := float64(weekInMilliseconds * t.averageConsumptionPerMillisecond)
	var records []*data_models.SensorValueRecord
	weekLongRecord := &data_models.SensorValueRecord{
		Id:                                  rand.Int(),
		BoxesSetID:                          rand.Int(),
		RecordDateUnix:                      utils.UnixToKievFormat(time.Now().Unix(), 0).Unix(),
		ValueAccumulationPeriodMilliseconds: weekInMilliseconds,
		SensorValue:                         consumed,
		PacketID:                            rand.Int(),
	}
	records = append(records, weekLongRecord)
	t.testForRecords(records)
}

// testForRecords accepts records in descending order by RecordDateUnix
func (t *appTestSuite) testForRecords(records []*data_models.SensorValueRecord) {
	earliestRecordTimeTruncatedUnix := t.app.getEarliestRecordTimeInTruncatedUnix(records)
	aggregationPeriods := data_models.NewAggregationPeriodsStorage()

	for _, record := range records {
		t.app.createAccumulationPeriodsAndDistributeConsumptionBetweenThem(
			record,
			aggregationPeriods,
			earliestRecordTimeTruncatedUnix,
			t.aggregationIntervalSeconds,
		)
	}
	aggregationPeriods.DeleteEmptyPeriods()

	firstAggregationPeriod := aggregationPeriods.Storage[0]
	t.r.Equal(
		earliestRecordTimeTruncatedUnix,
		firstAggregationPeriod.Data.EndUnix,
		"first aggregation period interval end incorrect")
	t.r.Equal(
		earliestRecordTimeTruncatedUnix-t.aggregationIntervalSeconds,
		firstAggregationPeriod.Data.StartUnix,
		"first aggregation period interval start incorrect",
	)

	lastRecord := records[len(records)-1]
	lastAggregationPeriod := aggregationPeriods.Storage[len(aggregationPeriods.Storage)-1]
	expectedStart := earliestRecordTimeTruncatedUnix - lastRecord.ValueAccumulationPeriodMilliseconds/1000
	t.r.Equal(
		expectedStart,
		lastAggregationPeriod.Data.StartUnix,
		"lastAggregationPeriod interval start incorrect",
	)
	t.r.Equal(
		expectedStart+t.aggregationIntervalSeconds,
		lastAggregationPeriod.Data.EndUnix,
		"lastAggregationPeriod interval start incorrect",
	)
}
