package worker_test

import (
	"testing"

	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/test"
	"github.com/mixer/redutil/worker"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type TaskSuite struct {
	*test.RedisSuite
}

func TestTaskSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &TaskSuite{test.NewSuite(pool)})
}

func (suite *TaskSuite) TestConstruction() {
	task := worker.NewTask(&MockLifecycle{}, []byte{})

	suite.Assert().IsType(&worker.Task{}, task)
}

func (suite *TaskSuite) TestBytesReturnsPayload() {
	task := worker.NewTask(&MockLifecycle{}, []byte("hello world"))

	payload := task.Bytes()

	suite.Assert().Equal([]byte("hello world"), payload)
}

func (suite *TaskSuite) TestDumpsBody() {
	task := worker.NewTask(&MockLifecycle{}, []byte("hello world"))
	expected := "00000000  68 65 6c 6c 6f 20 77 6f  72 6c 64                 |hello world|\n"
	suite.Assert().Equal(expected, task.HexDump())
}

func (suite *TaskSuite) TestStringReturns() {
	task := worker.NewTask(&MockLifecycle{}, []byte("hello world"))
	suite.Assert().Equal("hello world", task.String())
}

func (suite *TaskSuite) TestSucceedingDelegatesToLifecycle() {
	lifecycle := &MockLifecycle{}
	lifecycle.On("Complete", mock.Anything).Return(nil)

	task := worker.NewTask(lifecycle, []byte{})

	task.Succeed()

	lifecycle.AssertCalled(suite.T(), "Complete", task)
	suite.Assert().True(task.IsResolved())
}

func (suite *TaskSuite) TestFailingDelegatesToLifecycle() {
	lifecycle := &MockLifecycle{}
	lifecycle.On("Abandon", mock.Anything).Return(nil)

	task := worker.NewTask(lifecycle, []byte{})

	task.Fail()

	lifecycle.AssertCalled(suite.T(), "Abandon", task)
	suite.Assert().True(task.IsResolved())
}

func (suite *TaskSuite) TestSucceedingMultipleTimes() {
	lifecycle := &MockLifecycle{}
	lifecycle.On("Complete", mock.Anything).Return(nil).Once()

	task := worker.NewTask(lifecycle, []byte{})

	task.Succeed()
	task.Succeed()

	lifecycle.AssertNumberOfCalls(suite.T(), "Complete", 1)
	suite.Assert().True(task.IsResolved())
}

func (suite *TaskSuite) TestFailingMultipleTimes() {
	lifecycle := &MockLifecycle{}
	lifecycle.On("Abandon", mock.Anything).Return(nil).Once()

	task := worker.NewTask(lifecycle, []byte{})

	task.Fail()
	task.Fail()

	lifecycle.AssertNumberOfCalls(suite.T(), "Abandon", 1)
	suite.Assert().True(task.IsResolved())
}
