package queue_test

import (
	"errors"
	"testing"
	"time"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/queue"
	"github.com/WatchBeam/redutil/test"
	"github.com/garyburd/redigo/redis"
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
		mock.Anything, "foo", time.Second).
		Return([]byte("bar"), errors.New("error"))

	q := queue.NewBaseQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	payload, err := q.Pull(time.Second)

	suite.Assert().Equal([]byte("bar"), payload)
	suite.Assert().Equal("error", err.Error())
}

func (suite *ByteQueueSuite) TestConcatsDelegatesToProcessor() {
	processor := &MockProcessor{}
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(nil).Once()
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(redis.ErrNil).Once()

	q := queue.NewBaseQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	_, err := q.Concat("bar")

	suite.Assert().Equal(redis.ErrNil, err)
	processor.AssertExpectations(suite.T())
}

func (suite *ByteQueueSuite) TestConcatAbortsOnCommandError() {
	err := redis.Error("oh no!")
	processor := &MockProcessor{}
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(err).Once()

	q := queue.NewBaseQueue(suite.Pool, "foo")

	q.SetProcessor(processor)
	_, qerr := q.Concat("bar")
	suite.Assert().Equal(err, qerr)

	processor.AssertExpectations(suite.T())
}

func (suite *ByteQueueSuite) TestConcatRetriesOnCnxError() {
	processor := &MockProcessor{}
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(errors.New("some net error or something")).Once()
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(redis.ErrNil).Once()

	q := queue.NewBaseQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	_, err := q.Concat("bar")

	suite.Assert().Equal(redis.ErrNil, err)
	processor.AssertExpectations(suite.T())
}
