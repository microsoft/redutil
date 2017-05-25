package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/heartbeat"
	"github.com/mixer/redutil/queue"
	"github.com/mixer/redutil/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type JanitorSuite struct {
	*test.RedisSuite
}

func TestJanitorSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &JanitorSuite{test.NewSuite(pool)})
}

type mockJanitor struct {
	mock.Mock
}

func (m *mockJanitor) OnPreConcat(cnx redis.Conn, worker string) error {
	return m.Called(cnx, worker).Error(0)
}

func (m *mockJanitor) OnPostConcat(cnx redis.Conn, worker string) error {
	return m.Called(cnx, worker).Error(0)
}

func (suite *JanitorSuite) generate() (*janitorRunner, *mockJanitor) {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	run := [][]interface{}{
		{"HSET", "beats", "old", time.Now().UTC().Add(-time.Hour).Format(heartbeat.DefaultTimeFormat)},
		{"HSET", "beats", "new", time.Now().UTC().Add(time.Hour).Format(heartbeat.DefaultTimeFormat)},
		{"DEL", "available"},
		{"DEL", "available:worker_old"},
		{"RPUSH", "available:worker_old", []byte{1, 2, 3}},
	}

	for _, r := range run {
		if _, err := cnx.Do(r[0].(string), r[1:]...); err != nil {
			panic(err)
		}
	}

	available := queue.NewByteQueue(suite.Pool, "available")
	janitor := new(mockJanitor)

	runner := newJanitorRunner(
		suite.Pool,
		heartbeat.NewDetector("beats", suite.Pool, heartbeat.HashExpireyStrategy{}),
		janitor,
		available,
	)

	return runner, janitor
}

func (suite *JanitorSuite) TestJanitorSweepsTheDustSuccessfully() {
	runner, m := suite.generate()
	m.On("OnPreConcat", mock.Anything, "old").Return(nil).Once()
	m.On("OnPostConcat", mock.Anything, "old").Return(nil).Once()

	go func() {
		for err := range runner.errs {
			panic(err)
		}
	}()

	runner.runCleaning()

	suite.WithRedis(func(cnx redis.Conn) {
		// moved the queues successfully:
		size, err := redis.Int(cnx.Do("LLEN", "available:worker_old"))
		suite.Assert().Nil(err)
		suite.Assert().Zero(size)

		data, err := redis.Bytes(cnx.Do("RPOP", "available"))
		suite.Assert().Nil(err)
		suite.Assert().Equal([]byte{1, 2, 3}, data)

		data, err = redis.Bytes(cnx.Do("HGET", "beats", "old"))
		suite.Assert().Nil(data)

		_, err = redis.Bytes(cnx.Do("HGET", "beats", "new"))
		suite.Assert().Nil(err)
	})

	m.AssertExpectations(suite.T())
}

func (suite *JanitorSuite) TestAbortsIfPreConcatFails() {
	expectedErr := errors.New("oh no!")
	runner, m := suite.generate()
	m.On("OnPreConcat", mock.Anything, "old").Return(expectedErr).Once()

	go runner.runCleaning()
	suite.Assert().Equal(expectedErr, <-runner.errs)

	suite.WithRedis(func(cnx redis.Conn) {
		size, err := redis.Int(cnx.Do("LLEN", "available:worker_old"))
		suite.Assert().Nil(err)
		suite.Assert().Equal(1, size)

		data, err := redis.Bytes(cnx.Do("HGET", "beats", "old"))
		suite.Assert().NotNil(data)
	})

	m.AssertExpectations(suite.T())
}
