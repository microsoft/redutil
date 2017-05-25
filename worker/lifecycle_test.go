package worker_test

import (
	"github.com/mixer/redutil/queue"
	"github.com/mixer/redutil/worker"
	"github.com/stretchr/testify/mock"
)

type MockLifecycle struct {
	mock.Mock
}

var _ worker.Lifecycle = new(MockLifecycle)

func (l *MockLifecycle) SetQueues(availableTasks queue.Queue,
	workingTasks *queue.DurableQueue) {
	l.Called(availableTasks, workingTasks)
}

func (l *MockLifecycle) Complete(task *worker.Task) error {
	args := l.Called(task)
	return args.Error(0)
}

func (l *MockLifecycle) Abandon(task *worker.Task) error {
	args := l.Called(task)
	return args.Error(0)
}

func (l *MockLifecycle) AbandonAll() error {
	args := l.Called()
	return args.Error(0)
}

func (l *MockLifecycle) Listen() (<-chan *worker.Task, <-chan error) {
	args := l.Called()
	return args.Get(0).(chan *worker.Task), args.Get(1).(chan error)
}

func (l *MockLifecycle) StopListening() { l.Called() }

func (l *MockLifecycle) Await() { l.Called() }
