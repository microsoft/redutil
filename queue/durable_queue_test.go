package queue_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/queue"
	"github.com/WatchBeam/redutil/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type DurableQueueSuite struct {
	*test.RedisSuite
}

func TestDurableQueueSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &DurableQueueSuite{test.NewSuite(pool)})
}

func (suite *DurableQueueSuite) TestPullDelegatesToProcessor() {
	p := &MockProcessor{}
	p.On("PullTo",
		mock.Anything, "foo", "bar", time.Second).
		Return([]byte("baz"), fmt.Errorf("woot")).Once()

	q := queue.NewDurableQueue(suite.Pool, "foo", "bar")
	q.SetProcessor(p)
	data, err := q.Pull(time.Second)

	suite.Assert().Equal([]byte("baz"), data)
	suite.Assert().Equal("woot", err.Error())
}

func (suite *DurableQueueSuite) TestDestReturnsTheDestination() {
	q := queue.NewDurableQueue(suite.Pool, "foo", "bar")

	dest := q.Dest()

	suite.Assert().Equal("bar", dest)
}

func (suite *DurableQueueSuite) TestSetDestUpdatesTheDestination() {
	q := queue.NewDurableQueue(suite.Pool, "foo", "old")

	old := q.Dest()
	new := q.SetDest("new")

	suite.Assert().Equal("old", old)
	suite.Assert().Equal("new", new)
}
