package worker_test

import (
	"errors"
	"testing"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/test"
	"github.com/WatchBeam/redutil/worker"
	"github.com/stretchr/testify/suite"
)

type WorkerSuite struct {
	*test.RedisSuite
}

func TestWorkerSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &WorkerSuite{test.NewSuite(pool)})
}

func (suite *WorkerSuite) TestConstruction() {
	w := worker.New(suite.Pool, "queue", "worker_1")
	defer w.Close()

	suite.Assert().IsType(&worker.Worker{}, w)
}

func (suite *WorkerSuite) TestStartPropogatesProcessor() {
	c1, c2 := make(chan *worker.Task), make(chan error)
	defer func() {
		close(c1)
		close(c2)
	}()

	l := &MockLifecycle{}
	task := worker.NewTask(l, []byte("payload"))

	go func() {
		c1 <- task
		c2 <- errors.New("error")
	}()

	l.On("Listen").Return(c1, c2).Once()

	w := worker.NewWithLifecycle(suite.Pool, "queue", "worker_1", l)

	t, e := w.Start()

	suite.Assert().Equal(task, <-t)
	suite.Assert().Equal("error", (<-e).Error())
}

func (suite *WorkerSuite) TestCloseWaitsForCompletion() {
	c1, c2 := make(chan *worker.Task), make(chan error)
	defer func() {
		close(c1)
		close(c2)
	}()

	l := &MockLifecycle{}
	l.On("Await").Return()
	l.On("Listen").Return(c1, c2)
	l.On("StopListening").Return()

	w := worker.NewWithLifecycle(suite.Pool, "queue", "worker_1", l)

	w.Start()
	w.Close()

	l.AssertCalled(suite.T(), "Await")
}

func (suite *WorkerSuite) TestHaltDoesNotWaitForCompletion() {
	c1, c2 := make(chan *worker.Task), make(chan error)
	defer func() {
		close(c1)
		close(c2)
	}()

	l := &MockLifecycle{}
	l.On("Await").Return()
	l.On("AbandonAll").Return(nil).Once()
	l.On("Listen").Return(c1, c2)
	l.On("StopListening").Return()

	w := worker.NewWithLifecycle(suite.Pool, "queue", "worker_1", l)

	w.Start()
	w.Halt()

	l.AssertCalled(suite.T(), "Await")
	l.AssertCalled(suite.T(), "AbandonAll")
}
