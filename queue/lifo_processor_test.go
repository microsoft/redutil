package queue_test

import (
	"testing"

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
	first, e1 := queue.LIFO.Pull(cnx, "keyspace")
	second, e2 := queue.LIFO.Pull(cnx, "keyspace")
	third, e3 := queue.LIFO.Pull(cnx, "keyspace")

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

	queue.FIFO.PullTo(cnx, "keyspace2", "keyspace")

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
