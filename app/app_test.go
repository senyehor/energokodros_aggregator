package app

import (
	"aggregator/data_models"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"testing"
	"time"
)

const (
	second          int64 = 1
	minuteInSeconds       = 60 * second
	hourInSeconds         = 60 * minuteInSeconds
	dayInSeconds          = 24 * hourInSeconds
	weekInSeconds         = 7 * dayInSeconds
	millisecond     int64 = 1000
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
	rand.Seed(time.Now().Unix())
	t.r = t.Require()
	// 400 - PC consumption Watt 23 - PC`s count in r class  9 * hourInSeconds - working dayInSeconds length in seconds 7 - days in weekInSeconds
	t.averageConsumptionPerMillisecond = 400 * 23 / millisecond
	t.aggregationIntervalSeconds = hourInSeconds
	t.app = CreateApp()
}

func (t *appTestSuite) TestSixHoursIncomingRecordsAccumulationIntervalDefault() {
	startTime := time.Now().Unix()
	defaultInterval := 30 * second
	endTime := startTime + 6*hourInSeconds
	var records []*data_models.SensorValueRecord
	id := 0
	for startTime != endTime {
		accumulationPeriod := 30 * second * millisecond
		record := &data_models.SensorValueRecord{
			Id:                                  id,
			BoxesSetID:                          (id % 16) + 1,
			RecordDateInUnix:                    endTime,
			ValueAccumulationPeriodMilliseconds: accumulationPeriod,
			SensorValue:                         float64(t.averageConsumptionPerMillisecond * accumulationPeriod),
			PacketID:                            rand.Intn(1000),
		}
		records = append(records, record)
		endTime -= defaultInterval
		id++
	}
	t.testForRecords(records)
}

func (t *appTestSuite) TestDayLongAndTwoDaysLongRecords() {
	var records []*data_models.SensorValueRecord
	now := time.Now()
	dayInMilliseconds := dayInSeconds * millisecond
	twoDaysInMilliseconds := 2 * dayInMilliseconds
	twoDaysAfterNow := time.Unix(now.Unix()+twoDaysInMilliseconds/millisecond, 0).Unix()
	record := &data_models.SensorValueRecord{
		Id:                                  2,
		BoxesSetID:                          1,
		RecordDateInUnix:                    twoDaysAfterNow,
		ValueAccumulationPeriodMilliseconds: twoDaysInMilliseconds,
		SensorValue:                         float64(t.averageConsumptionPerMillisecond * twoDaysInMilliseconds),
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, record)
	record = &data_models.SensorValueRecord{
		Id:                                  1,
		BoxesSetID:                          1,
		RecordDateInUnix:                    now.Unix(),
		ValueAccumulationPeriodMilliseconds: dayInMilliseconds,
		SensorValue:                         float64(t.averageConsumptionPerMillisecond * dayInMilliseconds),
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, record)

	t.testForRecords(records)
}

func (t *appTestSuite) TestThreeDaysAccumulationInterval() {
	threeDaysInMilliseconds := dayInSeconds * 3 * millisecond
	consumed := float64(threeDaysInMilliseconds * t.averageConsumptionPerMillisecond)
	var records []*data_models.SensorValueRecord
	threeDaysLongRecord := &data_models.SensorValueRecord{
		Id:                                  rand.Intn(1000),
		BoxesSetID:                          rand.Intn(16),
		RecordDateInUnix:                    time.Now().Unix(),
		ValueAccumulationPeriodMilliseconds: threeDaysInMilliseconds,
		SensorValue:                         consumed,
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, threeDaysLongRecord)
	t.testForRecords(records)
}

func (t *appTestSuite) TestWeekAccumulationInterval() {
	weekInMilliseconds := weekInSeconds * millisecond
	consumed := float64(weekInMilliseconds * t.averageConsumptionPerMillisecond)
	var records []*data_models.SensorValueRecord
	id := 0
	weekLongRecord := &data_models.SensorValueRecord{
		Id:                                  id,
		BoxesSetID:                          rand.Intn(16),
		RecordDateInUnix:                    time.Now().Unix(),
		ValueAccumulationPeriodMilliseconds: weekInMilliseconds,
		SensorValue:                         consumed,
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, weekLongRecord)
	t.testForRecords(records)
}

// testForRecords accepts records in descending order by RecordDateInUnix
func (t *appTestSuite) testForRecords(records []*data_models.SensorValueRecord) {
	earliestRecordTimeInTruncatedUnix := t.app.getEarliestRecordTimeInTruncatedUnix(records)
	aggregationPeriods := data_models.NewAggregationPeriodsStorage()
	for _, record := range records {
		t.app.createAccumulationPeriodsAndDistributeConsumptionBetweenThem(
			record,
			aggregationPeriods,
			earliestRecordTimeInTruncatedUnix,
			t.aggregationIntervalSeconds,
		)
	}
	aggregationPeriods.DeleteEmptyPeriods()

	for _, record := range records {
		matched := false
		iterator := aggregationPeriods.Iter()
		for iterator.HasNext() {
			aggregationPeriod := iterator.GetAggregationPeriod()

		}
	}
}
