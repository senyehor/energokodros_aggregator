package app

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"time"
)

type appTestSuite struct {
	suite.Suite
	a *assert.Assertions
}

func (t *appTestSuite) SetupTest() {
	t.a = t.Assert()
}

func (t *appTestSuite) TestWeekAccumulationInterval() {

}

func (t *appTestSuite) TestTruncateToHourUnix() {
	currentTime := time.Now()
	truncatedTime := time.Unix(truncateToHourUnix(currentTime.Unix(), 0), 0)
	t.a.Equal(0, truncatedTime.Minute(), "minutes were not truncated")
	t.a.Equal(0, truncatedTime.Second(), "seconds were not truncated")
}
