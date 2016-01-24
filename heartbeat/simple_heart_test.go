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

type SimpleHeartbeatSuite struct {
	*test.RedisSuite
}

func TestSimpleHeartbeatSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &SimpleHeartbeatSuite{test.NewSuite(pool)})
}

func (suite *SimpleHeartbeatSuite) TestConstruction() {
	h := heartbeat.NewSimpleHeart("bar", "foo", time.Second, suite.Pool, heartbeat.HashExpireyStrategy{time.Second})
	defer h.Close()

	suite.Assert().IsType(heartbeat.SimpleHeart{}, h)
	suite.Assert().Equal(h.ID, "bar")
	suite.Assert().Equal(h.Location, "foo")
	suite.Assert().Equal(h.Interval, time.Second)
}

func (suite *SimpleHeartbeatSuite) TestStrategyIsCalledAtInterval() {
	strategy := &TestStrategy{}
	strategy.On("Touch", "foo", "bar", suite.Pool).Return(nil)

	h := heartbeat.NewSimpleHeart("bar", "foo", 5*time.Millisecond, suite.Pool, strategy)
	defer h.Close()

	time.Sleep(10 * time.Millisecond)

	strategy.AssertNumberOfCalls(suite.T(), "Touch", 1)
}

func (suite *SimpleHeartbeatSuite) TestStrategyPropogatesErrors() {
	strategy := &TestStrategy{}
	strategy.On("Touch", "foo", "bar", suite.Pool).Return(errors.New("some error"))

	h := heartbeat.NewSimpleHeart("bar", "foo", 100*time.Millisecond, suite.Pool, strategy)
	defer h.Close()

	errs := h.Errs()

	suite.Assert().Len(errs, 0)

	time.Sleep(150 * time.Millisecond)

	suite.Assert().Equal("some error", (<-errs).Error())
	suite.Assert().Len(errs, 0)
}

func (suite *SimpleHeartbeatSuite) TestCloseStopsCallingStrategy() {
	strategy := &TestStrategy{}
	strategy.On("Touch", "foo", "bar", suite.Pool).Return(nil)

	h := heartbeat.NewSimpleHeart("bar", "foo", 5*time.Millisecond, suite.Pool, strategy)
	h.Close()

	time.Sleep(10 * time.Millisecond)

	strategy.AssertNumberOfCalls(suite.T(), "Touch", 0)
}
