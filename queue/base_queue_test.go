package queue_test

import (
	"errors"
	"testing"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/queue"
	"github.com/WatchBeam/redutil/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type BaseQueueSuite struct {
	*test.RedisSuite
}

func TestBaseQueueSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &BaseQueueSuite{test.NewSuite(pool)})
}

func (suite *BaseQueueSuite) TestPushDelegatesToProcesor() {
	processor := &MockProcessor{}
	processor.
		On("Push",
		mock.Anything, "foo", []byte("payload")).
		Return(errors.New("error"))

	q := queue.NewBaseQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	err := q.Push([]byte("payload"))

	suite.Assert().Equal("error", err.Error())
	processor.AssertNumberOfCalls(suite.T(), "Push", 1)
}

func (suite *ByteQueueSuite) TestPullDelegatesToProcessor() {
	processor := &MockProcessor{}
	processor.On("Pull",
		mock.Anything, "foo").
		Return([]byte("bar"), errors.New("error"))

	q := queue.NewBaseQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	payload, err := q.Pull()

	suite.Assert().Equal([]byte("bar"), payload)
	suite.Assert().Equal("error", err.Error())
}
