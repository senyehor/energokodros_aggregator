package data_models

type SensorValueRecord struct {
	Id                             int
	BoxesSetID                     int
	RecordDateUnix                 int64
	ValueAccumulationPeriodSeconds int64
	SensorValue                    float64
	PacketID                       int
}

type AggregatedRecordsValues struct {
	boxesSetID                 int
	aggregationPeriodStartUnix int64
	aggregationPeriodEndUnix   int64
}
