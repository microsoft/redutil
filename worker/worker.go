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

	// availableTasks is a Redis queue that contains an in-order list of
	// tasks that need to be worked on. Workers race into this list.
	availableTasks queue.Queue
	// workingTasks contains the list of tasks that this particular worker
	// is currently working on. See above semantics as to where these items
	// move to and from.
	workingTasks *queue.DurableQueue

	lifecycle Lifecycle

	// The heartbeat components are used to maintain the state of the worker
	// and to detect dead workers to clean up.
	detector heartbeat.Detector
	heart    heartbeat.Heart

	// The janitor is responsible for cleaning up dead workers.
	janitor       Janitor
	janitorRunner *janitorRunner

	// smu wraps Worker#state in a loving, mutex-y embrace.
	smu sync.Mutex
	// The open or closed state of the worker. Locked by the cond.
	state state
}

const (
	// Default interval passed into heartbeat.New
	defaultHeartInterval = 10 * time.Second
	// Default interval we use to check for dead workers. Note that the first
	// check will be anywhere in the range [0, monitor interval]; this is
	// randomized so that workers that start at the same time will not
	// contest the same locks.
	defaultMonitorInterval = 120 * Second
)

// New creates and returns a pointer to a new instance of a Worker. It uses the
// given redis.Pool, the main queue's keyspace to pull from, and is given a
// unique ID through the `id` paramter.
func New(pool *redis.Pool, queue, id string) *Worker {
	heartbeater := heartbeat.New(
		id,
		fmt.Sprintf("%s:%s:%s", queue, "ticks", id),
		defaultHeartInterval, pool)

	workerName := fmt.Sprintf("%s:worker_%s", source, id)
	availableTasks := queue.NewByteQueue(pool, source)
	workingTasks := queue.NewDurableQueue(pool, source, workerName)

	return &Worker{
		pool:           pool,
		availableTasks: availableTasks,
		workingTasks:   workingTasks,

		lifecycle: NewLifecycle(pool, availableTasks, workingTasks),
		detector:  heartbeater.Detector(),
		heart:     heartbeater.Heart(),
		janitor:   nilJanitor{},

		state: closed,
	}
}

// Sets the Janitor interface used to dispose of old workers. This is optional;
// if you do not need to hook in extra functionality, you don't need to
// provide a janitor.
func (w *Worker) SetJanitor(janitor Janitor) {
	w.janitor = janitor
}

// Start signals the worker to begin receiving tasks from the main queue.
func (w *Worker) Start() (<-chan *Task, <-chan error) {
	w.smu.Lock()
	defer w.smu.Unlock()

	w.state = open
	w.janitorRunner = newJanitorRunner(w.pool, w.detector, w.janitor,
		w.availableTasks, w.workingTasks)

	errs1 := w.janitorRunner.Start()
	tasks, errs2 := w.lifecycle.Listen()

	return tasks, concatErrs(errs1, errs2)
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
	w.janitorRunner.Close()

	fn()

	w.lifecycle.Await()

	w.state = closed
}
