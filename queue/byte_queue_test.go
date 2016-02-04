package queue_test

import (
	"errors"
	"testing"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/queue"
	"github.com/WatchBeam/redutil/test"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/mock"
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

func (suite *ByteQueueSuite) TestConcatsDelegatesToProcessor() {
	processor := &MockProcessor{}
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(nil).Once()
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(redis.ErrNil).Once()

	q := queue.NewByteQueue(suite.Pool, "foo")

	q.SetProcessor(processor)
	suite.Assert().Nil(q.Concat("bar"))

	processor.AssertExpectations(suite.T())
}

func (suite *ByteQueueSuite) TestConcatAbortsOnCommandError() {
	err := redis.Error("oh no!")
	processor := &MockProcessor{}
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(err).Once()

	q := queue.NewByteQueue(suite.Pool, "foo")

	q.SetProcessor(processor)
	suite.Assert().Equal(q.Concat("bar"), err)

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

	q := queue.NewByteQueue(suite.Pool, "foo")

	q.SetProcessor(processor)
	suite.Assert().Nil(q.Concat("bar"))

	processor.AssertExpectations(suite.T())
}

func (suite *ByteQueueSuite) TestConcatAbortsOnTooManyErrors() {
	err := errors.New("some net error or something")
	processor := &MockProcessor{}
	processor.On("Concat",
		mock.Anything, "bar", "foo").
		Return(err).Times(3)

	q := queue.NewByteQueue(suite.Pool, "foo")

	q.SetProcessor(processor)
	suite.Assert().Equal(q.Concat("bar"), err)

	processor.AssertExpectations(suite.T())
}
