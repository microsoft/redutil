package heartbeat_test

import (
	"errors"
	"testing"
	"time"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/heartbeat"
	"github.com/WatchBeam/redutil/test"
	"github.com/stretchr/testify/suite"
)

type SimpleDetectorSuite struct {
	*test.RedisSuite
}

func TestSimpleDetectorSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &SimpleDetectorSuite{test.NewSuite(pool)})
}

func (suite *SimpleDetectorSuite) TestConstruction() {
	d := heartbeat.NewDetector("foo", suite.Pool, heartbeat.HashExpireyStrategy{time.Second})

	suite.Assert().IsType(heartbeat.SimpleDetector{}, d)
}

func (suite *SimpleDetectorSuite) TestDetectDelegatesToStrategy() {
	strategy := &TestStrategy{}
	strategy.On("Expired", "foo", suite.Pool).Return([]string{}, nil)

	d := heartbeat.NewDetector("foo", suite.Pool, strategy)
	d.Detect()

	strategy.AssertCalled(suite.T(), "Expired", "foo", suite.Pool)
}

func (suite *SimpleDetectorSuite) TestDetectPropogatesValues() {
	strategy := &TestStrategy{}
	strategy.On("Expired", "foo", suite.Pool).Return([]string{"foo", "bar"}, errors.New("baz"))

	d := heartbeat.NewDetector("foo", suite.Pool, strategy)
	expired, err := d.Detect()

	suite.Assert().Equal(expired, []string{"foo", "bar"})
	suite.Assert().Equal(err.Error(), "baz")
}
