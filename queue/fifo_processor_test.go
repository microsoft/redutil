package queue_test

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/queue"
	"github.com/mixer/redutil/test"
	"github.com/stretchr/testify/suite"
)

type FIFOProcessorTest struct {
	*test.RedisSuite
}

func TestFIFOProcessorSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &FIFOProcessorTest{test.NewSuite(pool)})
}

func (suite *FIFOProcessorTest) assertOrder(cnx redis.Conn) {
	first, e1 := queue.FIFO.Pull(cnx, "keyspace", time.Second)
	second, e2 := queue.FIFO.Pull(cnx, "keyspace", time.Second)
	third, e3 := queue.FIFO.Pull(cnx, "keyspace", time.Second)

	suite.Assert().Equal([]byte("first"), first)
	suite.Assert().Equal([]byte("second"), second)
	suite.Assert().Equal([]byte("third"), third)

	suite.Assert().Nil(e1)
	suite.Assert().Nil(e2)
	suite.Assert().Nil(e3)
}

func (suite *FIFOProcessorTest) TestPullToOrder() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	queue.FIFO.Push(cnx, "keyspace", []byte("first"))
	queue.FIFO.Push(cnx, "keyspace", []byte("second"))
	queue.FIFO.Push(cnx, "keyspace2", []byte("third"))

	queue.FIFO.PullTo(cnx, "keyspace2", "keyspace", time.Second)

	suite.assertOrder(cnx)
}

func (suite *FIFOProcessorTest) TestProcessingOrder() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	queue.FIFO.Push(cnx, "keyspace", []byte("first"))
	queue.FIFO.Push(cnx, "keyspace", []byte("second"))
	queue.FIFO.Push(cnx, "keyspace", []byte("third"))

	suite.assertOrder(cnx)
}

func (suite *FIFOProcessorTest) TestConcats() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	queue.FIFO.Push(cnx, "keyspace", []byte("first"))
	queue.FIFO.Push(cnx, "keyspace2", []byte("second"))
	queue.FIFO.Push(cnx, "keyspace2", []byte("third"))

	suite.Assert().Nil(queue.FIFO.Concat(cnx, "keyspace2", "keyspace"))
	suite.Assert().Nil(queue.FIFO.Concat(cnx, "keyspace2", "keyspace"))
	suite.Assert().Equal(redis.ErrNil, queue.FIFO.Concat(cnx, "keyspace2", "keyspace"))

	suite.assertOrder(cnx)
}

// Unfortunately, this test takes a lot of time to run since redis does not
// support floating point timeouts.
func (suite *FIFOProcessorTest) TestPullRespectsTimeouts() {
	cnx := suite.Pool.Get()
	defer cnx.Close()

	b, err := queue.FIFO.Pull(cnx, "keyspace", 250*time.Millisecond)

	suite.Assert().Empty(b)
	suite.Assert().Equal(redis.ErrNil, err)
}
