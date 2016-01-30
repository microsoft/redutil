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

type MockProcessor struct {
	mock.Mock
}

func (m *MockProcessor) Push(cnx redis.Conn, key string, payload []byte) error {
	args := m.Called(cnx, key, payload)
	return args.Error(0)
}

func (m *MockProcessor) Pull(cnx redis.Conn, key string) ([]byte, error) {
	args := m.Called(cnx, key)
	return args.Get(0).([]byte), args.Error(1)
}

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

func (suite *ByteQueueSuite) TestPushDelegatesToProcesor() {
	processor := &MockProcessor{}
	processor.
		On("Push",
		mock.Anything, "foo", []byte("payload")).
		Return(nil)

	q := queue.NewByteQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	q.Push([]byte("payload"))
	processor.AssertNumberOfCalls(suite.T(), "Push", 1)
}

func (suite *ByteQueueSuite) TestPushPropogatesErrors() {
	processor := &MockProcessor{}
	processor.
		On("Push",
		mock.Anything, "foo", []byte("payload")).
		Return(errors.New("error"))

	q := queue.NewByteQueue(suite.Pool, "foo")
	q.SetProcessor(processor)

	err := q.Push([]byte("payload"))
	suite.Assert().Equal("error", err.Error())
}

func (suite *ByteQueueSuite) TestPullDelegatesToProcessor() {
	processor := &MockProcessor{}
	processor.On("Pull",
		mock.Anything, "foo").
		Return([]byte("bar"), nil).Once()
	processor.On("Pull",
		mock.Anything, "foo").
		Return([]byte{}, redis.ErrNil).After(1 * time.Second).Once()

	q := queue.NewByteQueue(suite.Pool, "foo")

	q.SetProcessor(processor)
	q.BeginRecv()
	defer q.Close()

	suite.Assert().Equal([]byte("bar"), <-q.In())
}
