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
	// 400 - PC consumption Watt 23 - PC`s count in a class  9 * hourInSeconds - working dayInSeconds
	// length in seconds 7 - days in weekInSeconds
	t.averageConsumptionPerMillisecond = 400 * 23 / millisecond
	t.aggregationIntervalSeconds = hourInSeconds
	t.app = CreateApp(utils.GetAppConfig())
}

func (t *appTestSuite) TestRealLifeRecordsSet() {
	var records []*data_models.SensorValueRecord
	id := 0
	timeIn := time.Now().Unix()

	for i := 0; i < 300; i++ {
		rand.Seed(time.Now().UnixNano())
		accumulationPeriod := t.generateRandomAccumulationInterval()
		// creating guaranteed cases
		if i > 20 && i < 30 {
			accumulationPeriod = 30 * second * millisecond
		}
		if i > 100 && i < 105 {
			accumulationPeriod = hourInSeconds * millisecond
		}
		if i > 200 && i < 202 {
			accumulationPeriod = dayInSeconds * millisecond
		}
		record := &data_models.SensorValueRecord{
			Id:                                  id,
			BoxesSetID:                          rand.Intn(32),
			RecordInsertedTimeUnix:              timeIn,
			ValueAccumulationPeriodMilliseconds: accumulationPeriod,
			SensorValue:                         float64(t.averageConsumptionPerMillisecond * accumulationPeriod),
			PacketID:                            rand.Intn(1000),
		}
		records = append(records, record)
		timeIn -= accumulationPeriod / millisecond
	}
	t.testForRecords(records)
}

func (t *appTestSuite) TestTwoIntervalsCreatedForAlmostRoundTimeInserted() {
	timeIn := time.Unix(truncateToHourUnix(time.Now().Unix(), 0), 0)
	timeIn = timeIn.Add(time.Second * 3)
	smallAccumulationPeriod := 10 * second * millisecond

	var records []*data_models.SensorValueRecord
	record := &data_models.SensorValueRecord{
		Id:                                  0,
		BoxesSetID:                          rand.Intn(32),
		RecordInsertedTimeUnix:              timeIn.Unix(),
		ValueAccumulationPeriodMilliseconds: smallAccumulationPeriod,
		SensorValue:                         float64(t.averageConsumptionPerMillisecond * smallAccumulationPeriod),
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, record)
	aggregationPeriods := t.app.processRecords(records)
	intervalsCount := 0
	iterator := aggregationPeriods.Iter()
	for iterator.HasNext() {
		intervalsCount++
	}
	t.r.Equal(2, intervalsCount, "incorrect intervals count was created")
}

func (t *appTestSuite) TestSixHoursIncomingRecordsAccumulationIntervalDefault() {
	earliestRecordTime := time.Now().Unix()
	defaultInterval := 30 * second
	recordTime := earliestRecordTime + 6*hourInSeconds
	var records []*data_models.SensorValueRecord
	id := 0
	for earliestRecordTime != recordTime {
		accumulationPeriod := 30 * second * millisecond
		record := &data_models.SensorValueRecord{
			Id:                                  id,
			BoxesSetID:                          (id % 16) + 1,
			RecordInsertedTimeUnix:              recordTime,
			ValueAccumulationPeriodMilliseconds: accumulationPeriod,
			SensorValue:                         float64(t.averageConsumptionPerMillisecond * accumulationPeriod),
			PacketID:                            rand.Intn(1000),
		}
		records = append(records, record)
		recordTime -= defaultInterval
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
		RecordInsertedTimeUnix:              twoDaysAfterNow,
		ValueAccumulationPeriodMilliseconds: twoDaysInMilliseconds,
		SensorValue:                         float64(t.averageConsumptionPerMillisecond * twoDaysInMilliseconds),
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, record)
	record = &data_models.SensorValueRecord{
		Id:                                  1,
		BoxesSetID:                          1,
		RecordInsertedTimeUnix:              now.Unix(),
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
		Id:                                  0,
		BoxesSetID:                          rand.Intn(16),
		RecordInsertedTimeUnix:              time.Now().Unix(),
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
		RecordInsertedTimeUnix:              time.Now().Unix(),
		ValueAccumulationPeriodMilliseconds: weekInMilliseconds,
		SensorValue:                         consumed,
		PacketID:                            rand.Intn(1000),
	}
	records = append(records, weekLongRecord)
	t.testForRecords(records)
}

// testForRecords accepts records in descending order by RecordInsertedTimeUnix
func (t *appTestSuite) testForRecords(records []*data_models.SensorValueRecord) {
	aggregationPeriods := t.app.processRecords(records)

	var foundAggregationPeriodForStart, foundAggregationPeriodForEnd bool
	for _, record := range records {
		foundAggregationPeriodForStart = false
		foundAggregationPeriodForEnd = false
		AggregationPeriodDataForRecordAccumulationStart, AggregationPeriodDataForRecordAccumulationEnd :=
			t.createExpectedIntervalsForRecord(record)

		iterator := aggregationPeriods.Iter()
		for iterator.HasNext() {
			aggregationPeriod := iterator.GetAggregationPeriod()

			if !foundAggregationPeriodForStart {
				if aggregationPeriod.Data.Equal(AggregationPeriodDataForRecordAccumulationStart) {
					foundAggregationPeriodForStart = true
				}
			}

			if !foundAggregationPeriodForEnd {
				if aggregationPeriod.Data.Equal(AggregationPeriodDataForRecordAccumulationEnd) {
					foundAggregationPeriodForEnd = true
					if foundAggregationPeriodForStart {
						break
					}
					continue

				}
			}
		}
		t.r.True(foundAggregationPeriodForStart, "aggregation interval for period start was not found")
		t.r.True(foundAggregationPeriodForEnd, "aggregation interval for period end was not found")
	}
}

// createExpectedIntervalsForRecord return expected interval start and end
func (t *appTestSuite) createExpectedIntervalsForRecord(
	record *data_models.SensorValueRecord,
) (*data_models.AggregationPeriodData, *data_models.AggregationPeriodData) {

	recordAccumulationStartUnix :=
		record.RecordInsertedTimeUnix - record.ValueAccumulationPeriodMilliseconds/millisecond
	expectedRecordAccumulationStartAggregationIntervalStart :=
		truncateToHourUnix(recordAccumulationStartUnix, 0)
	expectedRecordAccumulationStartAggregationIntervalEnd :=
		expectedRecordAccumulationStartAggregationIntervalStart + t.aggregationIntervalSeconds

	expectedAggregationIntervalForRecordStart := data_models.NewAggregationPeriodData(
		record.BoxesSetID,
		expectedRecordAccumulationStartAggregationIntervalEnd,
		t.app.aggregationIntervalSeconds,
	)

	expectedRecordAccumulationEndAggregationIntervalStart :=
		truncateToHourUnix(record.RecordInsertedTimeUnix, 0)
	expectedRecordAccumulationEndAggregationIntervalEnd :=
		expectedRecordAccumulationEndAggregationIntervalStart + t.aggregationIntervalSeconds

	expectedAggregationIntervalForRecordEnd := data_models.NewAggregationPeriodData(
		record.BoxesSetID,
		expectedRecordAccumulationEndAggregationIntervalEnd,
		t.app.aggregationIntervalSeconds,
	)
	return expectedAggregationIntervalForRecordStart, expectedAggregationIntervalForRecordEnd
}

func (t *appTestSuite) generateRandomAccumulationInterval() int64 {
	rand.Seed(time.Now().UnixNano())
	probability := rand.Intn(101)
	if probability < 70 { // 70% chance
		return 30 * second * millisecond
	}
	if probability < 90 { // 30% chance
		return (rand.Int63n(4) + 1) * minuteInSeconds * millisecond
	}
	if probability < 98 { // 15% chance
		return (rand.Int63n(3)+1)*hourInSeconds*millisecond +
			(rand.Int63n(10) * minuteInSeconds * millisecond)
	}
	// 5% chance
	return (rand.Int63n(10) + 10) * hourInSeconds * millisecond
}
