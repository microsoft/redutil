package queue_test

import (
	"testing"

	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/queue"
	"github.com/mixer/redutil/test"
	"github.com/stretchr/testify/suite"
)

type ByteQueueSuite struct {
	*test.RedisSuite
}

func TestByteQueueSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &ByteQueueSuite{test.NewSuite(pool)})
}

func (suite *ByteQueueSuite) TestConstruction() {
	q := queue.NewByteQueue(suite.Pool, "foo")

	suite.Assert().IsType(&queue.ByteQueue{}, q)
}
