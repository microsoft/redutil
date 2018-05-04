package heartbeat_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/heartbeat"
	"github.com/mixer/redutil/test"
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

func (suite *SimpleHeartbeatSuite) TestStrategyIsCalledAtInitializationAndInterval() {
	strategy := &TestStrategy{}
	strategy.On("Touch", "foo", "bar", suite.Pool).Return(nil)

	h := heartbeat.NewSimpleHeart("bar", "foo", 50*time.Millisecond, suite.Pool, strategy)
	strategy.AssertNumberOfCalls(suite.T(), "Touch", 1)
	defer h.Close()

	time.Sleep(60 * time.Millisecond)

	strategy.AssertNumberOfCalls(suite.T(), "Touch", 2)
}

func (suite *SimpleHeartbeatSuite) TestStrategyPropogatesErrors() {
	strategy := &TestStrategy{}
	err := errors.New("some error")
	strategy.On("Touch", "foo", "bar", suite.Pool).Twice().Return(err)

	h := heartbeat.NewSimpleHeart("bar", "foo", 100*time.Millisecond, suite.Pool, strategy)
	defer h.Close()

	errs := h.Errs()
	suite.Assert().Len(errs, 1)
	suite.Assert().Equal(err, <-errs)

	time.Sleep(150 * time.Millisecond)

	suite.Assert().Equal(err, <-errs)
	suite.Assert().Len(errs, 0)
}

func (suite *SimpleHeartbeatSuite) TestCloseStopsCallingStrategy() {
	strategy := &TestStrategy{}
	strategy.On("Touch", "foo", "bar", suite.Pool).Return(nil)

	h := heartbeat.NewSimpleHeart("bar", "foo", 5*time.Millisecond, suite.Pool, strategy)
	h.Close()

	time.Sleep(10 * time.Millisecond)

	strategy.AssertNumberOfCalls(suite.T(), "Touch", 1)
}
