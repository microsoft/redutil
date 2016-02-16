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

type HashExpireyStrategySuite struct {
	*test.RedisSuite
}

func TestHashExpireyStrategySuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &HashExpireyStrategySuite{test.NewSuite(pool)})
}

func (suite *HashExpireyStrategySuite) TestTouchAddsValues() {
	s := &heartbeat.HashExpireyStrategy{MaxAge: time.Second}

	s.Touch("foo", "bar", suite.Pool)

	suite.WithRedis(func(cnx redis.Conn) {
		exists, err := redis.Bool(cnx.Do("HEXISTS", "foo", "bar"))

		suite.Assert().Nil(err, "HEXISTS: expected no error but got one")
		suite.Assert().True(exists, "HEXISTS: expected to find foo:bar but didn't")
	})
}

func (suite *HashExpireyStrategySuite) TestExpiredFindsDeadValues() {
	s := &heartbeat.HashExpireyStrategy{MaxAge: time.Second}

	suite.WithRedis(func(cnx redis.Conn) {
		tick := time.Now().UTC().Add(-10 * time.Second).Format(heartbeat.DefaultTimeFormat)
		cnx.Do("HSET", "foo", "bar", tick)

		expired, err := s.Expired("foo", suite.Pool)

		suite.Assert().Nil(err)
		suite.Assert().Len(expired, 1)
		suite.Assert().Equal(expired[0], "bar")
	})
}

func (suite *HashExpireyStrategySuite) TestExpiredIgnoresUnreadableValues() {
	s := &heartbeat.HashExpireyStrategy{MaxAge: time.Second}

	suite.WithRedis(func(cnx redis.Conn) {
		tick := time.Now().UTC().Add(-10 * time.Second).Format(heartbeat.DefaultTimeFormat)
		cnx.Do("HSET", "foo", "bar", tick)
		cnx.Do("HSET", "foo", "baz", "not a real time")

		expired, err := s.Expired("foo", suite.Pool)

		suite.Assert().Nil(err)
		suite.Assert().Len(expired, 1)
		suite.Assert().Equal(expired[0], "bar")
	})
}
