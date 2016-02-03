package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/WatchBeam/redutil/heartbeat"
	"github.com/garyburd/redigo/redis"
)

// Internal state tracking used in the worker.
type state uint8

const (
	open    state = iota // tasks are running or can be run on the worker
	halting              // we're terminating all ongoing tasks
	closing              // we're waiting for tasks to gracefully close
	closed               // all tasks have terminated
)

// A Worker is the bridge between Redutil Queues and the worker pattern. Items
// can be moved around between different queues using a lifecycle (see
// Lifecycle, DefaultLifecycle), and worked on by clients. "Dead" workers' items
// are recovered by other, living ones, providing an in-order, reliable,
// distributed implementation of the worker pattern.
type Worker struct {
	pool *redis.Pool

	// lifecycle is the lifecycle
	lifecycle Lifecycle
	heart     heartbeat.Heart

	// smu wraps Worker#state in a loving, mutex-y embrace.
	smu sync.Mutex
	// The open or closed state of the worker. Locked by the cond.
	state state
}

const defaultHeartInterval = 3 * time.Second

// New creates and returns a pointer to a new instance of a Worker. It uses the
// given redis.Pool, the main queue's keyspace to pull from, and is given a
// unique ID through the `id` paramter.
func New(pool *redis.Pool, queue, id string) *Worker {
	heartbeater := heartbeat.New(
		id,
		fmt.Sprintf("%s:%s:%s", queue, "ticks", id),
		defaultHeartInterval, pool)

	return &Worker{
		pool:      pool,
		lifecycle: NewLifecycle(pool, queue, id),
		heart:     heartbeater.Heart(),
		state:     closed,
	}
}

func NewWithLifecycle(pool *redis.Pool, queue, id string, lifecycle Lifecycle) *Worker {
	w := New(pool, queue, id)
	w.lifecycle = lifecycle

	return w
}

// Start signals the worker to begin receiving tasks from the main queue.
func (w *Worker) Start() (<-chan *Task, <-chan error) {
	w.smu.Lock()
	defer w.smu.Unlock()

	w.state = open

	return w.lifecycle.Listen()
}

// Close stops polling the queue immediately and waits for all tasks to complete
// before stopping the heartbeat.
func (w *Worker) Close() {
	w.startClosing(func() {
		w.state = closing
	})
}

// Halt stops the heartbeat and queue polling goroutines immediately and cancels
// all tasks, marking them as FAILED before returning.
func (w *Worker) Halt() {
	w.startClosing(func() {
		w.state = halting
		w.lifecycle.AbandonAll()
	})
}

// Starts closing the worker if it was not already closed. Invokes the passed
// function to help in the teardown, and blocks until all tasks are done.
func (w *Worker) startClosing(fn func()) {
	w.smu.Lock()
	defer w.smu.Unlock()
	if w.state != open {
		return
	}

	w.lifecycle.StopListening()
	w.heart.Close()

	fn()

	w.lifecycle.Await()

	w.state = closed
}
