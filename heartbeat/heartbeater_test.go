package heartbeat_test

import (
	"testing"
	"time"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/heartbeat"
	"github.com/WatchBeam/redutil/test"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/suite"
)

type HeartbeaterSuite struct {
	*test.RedisSuite
}

type StrategyStub struct{}

func (_ *StrategyStub) Touch(location, ID string, pool *redis.Pool) (err error) {
	return nil
}

func (_ *StrategyStub) Expired(location string, pool *redis.Pool) (expired []string, err error) {
	return make([]string, 0), nil
}

func TestHeartbeaterSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &HeartbeaterSuite{test.NewSuite(pool)})
}

func (suite *HeartbeaterSuite) TestConstruction() {
	h := heartbeat.New("foo", "bar", time.Second, suite.Pool)

	suite.Assert().Equal(h.ID, "foo")
	suite.Assert().Equal(h.Location, "bar")
	suite.Assert().Equal(h.Interval(), time.Second)
}

func (suite *HeartbeaterSuite) TestMaxAgePadsSmallValues() {
	h := heartbeat.New("foo", "bar", 3*time.Second, suite.Pool)

	suite.Assert().Equal(h.MaxAge(), 4*time.Second)
}

func (suite *HeartbeaterSuite) TestMaxAgeScalesLargeValues() {
	h := heartbeat.New("foo", "bar", 10*time.Second, suite.Pool)

	suite.Assert().Equal(h.MaxAge(), 15*time.Second)
}

func (suite *HeartbeaterSuite) TestSetStrategy() {
	h := heartbeat.New("foo", "bar", 10*time.Second, suite.Pool)
	strategy := &StrategyStub{}

	h.SetStrategy(strategy)

	suite.Assert().Equal(h.Strategy, strategy)
}

func (suite *HeartbeaterSuite) TestHeartCreation() {
	h := heartbeat.New("foo", "bar", 10*time.Second, suite.Pool)

	heart, ok := h.Heart().(heartbeat.SimpleHeart)

	suite.Assert().True(ok, "heart: expected heart to be heartbeat.SimpleHeart")
	suite.Assert().Equal(heart.ID, "foo")
	suite.Assert().Equal(heart.Location, "bar")
	suite.Assert().Equal(heart.Interval, 10*time.Second)
}

func (suite *HeartbeaterSuite) TestDetectorCreation() {
	h := heartbeat.New("foo", "bar", 10*time.Second, suite.Pool)
	strategy := &StrategyStub{}
	h.SetStrategy(strategy)

	detector, ok := h.Detector().(heartbeat.SimpleDetector)

	suite.Assert().True(ok, "detector: expected detector to be heartbeat.SimpleDetector")
	suite.Assert().Equal(detector.Location(), "bar")
	suite.Assert().Equal(detector.Strategy(), strategy)
}
