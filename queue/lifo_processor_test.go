package queue_test

import (
	"testing"
	"time"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/queue"
	"github.com/WatchBeam/redutil/test"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/suite"
)

type LIFOProcessorTest struct {
	*test.RedisSuite
}

func TestLIFOProcessorSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &LIFOProcessorTest{test.NewSuite(pool)})
}

func (suite *LIFOProcessorTest) assertOrder(cnx redis.Conn) {
	first, e1 := queue.LIFO.Pull(cnx, "keyspace", time.Second)
	second, e2 := queue.LIFO.Pull(cnx, "keyspace", time.Second)
	third, e3 := queue.LIFO.Pull(cnx, "keyspace", time.Second)

	suite.Assert().Equal([]byte("third"), first)
	suite.Assert().Equal([]byte("second"), second)
	suite.Assert().Equal([]byte("first"), third)

	suite.Assert().Nil(e1)
	suite.Assert().Nil(e2)
	suite.Assert().Nil(e3)
}

func (suite *LIFOProcessorTest) TestPullToOrder() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	queue.FIFO.Push(cnx, "keyspace", []byte("third"))
	queue.FIFO.Push(cnx, "keyspace", []byte("second"))
	queue.FIFO.Push(cnx, "keyspace2", []byte("first"))

	queue.FIFO.PullTo(cnx, "keyspace2", "keyspace", time.Second)

	suite.assertOrder(cnx)
}

func (suite *LIFOProcessorTest) TestProcessingOrder() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	queue.LIFO.Push(cnx, "keyspace", []byte("first"))
	queue.LIFO.Push(cnx, "keyspace", []byte("second"))
	queue.LIFO.Push(cnx, "keyspace", []byte("third"))

	suite.assertOrder(cnx)
}

func (suite *LIFOProcessorTest) TestConcats() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	queue.LIFO.Push(cnx, "keyspace", []byte("first"))
	queue.LIFO.Push(cnx, "keyspace2", []byte("second"))
	queue.LIFO.Push(cnx, "keyspace2", []byte("third"))

	suite.Assert().Nil(queue.LIFO.Concat(cnx, "keyspace2", "keyspace"))
	suite.Assert().Nil(queue.LIFO.Concat(cnx, "keyspace2", "keyspace"))
	suite.Assert().Equal(redis.ErrNil, queue.LIFO.Concat(cnx, "keyspace2", "keyspace"))

	suite.assertOrder(cnx)
}

// Unfortunately, this test takes a lot of time to run since redis does not
// support floating point timeouts.
func (suite *LIFOProcessorTest) TestPullRespectsTimeouts() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	b, err := queue.LIFO.Pull(cnx, "empty_keyspace", 250*time.Millisecond)

	suite.Assert().Empty(b)
	suite.Assert().Equal(redis.ErrNil, err)
}
