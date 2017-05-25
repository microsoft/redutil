package worker_test

import (
	"errors"
	"testing"

	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/queue"
	"github.com/mixer/redutil/test"
	"github.com/mixer/redutil/worker"
	"github.com/stretchr/testify/mock"
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

	suite.Assert().IsType(&worker.DefaultWorker{}, w)
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

	l.On("SetQueues", mock.Anything, mock.Anything).Return()
	l.On("Listen").Return(c1, c2).Once()

	w := worker.New(suite.Pool, "queue", "worker_1")
	w.SetLifecycle(l)

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
	l.On("SetQueues", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		src := args.Get(0).(queue.Queue)
		suite.Assert().Equal("queue", src.Source())
		dest := args.Get(1).(*queue.DurableQueue)
		suite.Assert().Equal("queue", dest.Source())
		suite.Assert().Equal("queue:worker_worker_1", dest.Dest())
	})

	w := worker.New(suite.Pool, "queue", "worker_1")
	w.SetLifecycle(l)

	w.Start()
	w.Close()

	l.AssertExpectations(suite.T())
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
	l.On("SetQueues", mock.Anything, mock.Anything).Return()

	w := worker.New(suite.Pool, "queue", "worker_1")
	w.SetLifecycle(l)

	w.Start()
	w.Halt()

	l.AssertCalled(suite.T(), "Await")
	l.AssertCalled(suite.T(), "AbandonAll")
}
