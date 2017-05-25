package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mixer/redutil/heartbeat"
	"github.com/mixer/redutil/queue"
)

// Internal state tracking used in the worker.
type state uint8

const (
	idle    state = iota // we've not yet started
	open                 // tasks are running or can be run on the worker
	halting              // we're terminating all ongoing tasks
	closing              // we're waiting for tasks to gracefully close
	closed               // all tasks have terminated
)

// A DefaultWorker is the bridge between Redutil Queues and the worker pattern. Items
// can be moved around between different queues using a lifecycle (see
// Lifecycle, DefaultLifecycle), and worked on by clients. "Dead" workers' items
// are recovered by other, living ones, providing an in-order, reliable,
// distributed implementation of the worker pattern.
type DefaultWorker struct {
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
	defaultMonitorInterval = 15 * time.Second
)

// Returns the name of the working queue based on the worker's processing
// source and worker ID. This is purposely NOT readily configurable; this
// is not something you have to touch for 99% of redutil usage, and
// incorrectly configuring this can result in Bad Things (dropped
// jobs, duplicate jobs, etc).
func getWorkingQueueName(src, id string) string {
	return fmt.Sprintf("%s:worker_%s", src, id)
}

// New creates and returns a pointer to a new instance of a DefaultWorker. It uses the
// given redis.Pool, the main queue to pull from (`src`), and is given a
// unique ID through the `id` paramter.
func New(pool *redis.Pool, src, id string) *DefaultWorker {
	heartbeater := heartbeat.New(
		id,
		fmt.Sprintf("%s:%s", src, "ticks"),
		defaultHeartInterval, pool)

	return &DefaultWorker{
		pool:           pool,
		availableTasks: queue.NewByteQueue(pool, src),
		workingTasks:   queue.NewDurableQueue(pool, src, getWorkingQueueName(src, id)),

		lifecycle: NewLifecycle(pool),
		detector:  heartbeater.Detector(),
		heart:     heartbeater.Heart(),
		janitor:   nilJanitor{},

		state: closed,
	}
}

// Sets the Lifecycle used for managing job states. Note: this is only safe
// to call BEFORE calling Start()
func (w *DefaultWorker) SetLifecycle(lf Lifecycle) {
	w.ensureUnstarted()
	w.lifecycle = lf
}

// Sets the Janitor interface used to dispose of old workers. This is optional;
// if you do not need to hook in extra functionality, you don't need to
// provide a janitor.
func (w *DefaultWorker) SetJanitor(janitor Janitor) {
	w.ensureUnstarted()
	w.janitor = janitor
}

func (w *DefaultWorker) ensureUnstarted() {
	w.smu.Lock()
	defer w.smu.Unlock()

	if w.state == open {
		panic("Attempted to alter the worker while it was running.")
	}
}

// Start signals the worker to begin receiving tasks from the main queue.
func (w *DefaultWorker) Start() (<-chan *Task, <-chan error) {
	w.smu.Lock()
	defer w.smu.Unlock()

	w.state = open
	w.lifecycle.SetQueues(w.availableTasks, w.workingTasks)
	w.janitorRunner = newJanitorRunner(w.pool, w.detector, w.janitor, w.availableTasks)

	errs1 := w.janitorRunner.Start()
	tasks, errs2 := w.lifecycle.Listen()

	return tasks, concatErrs(errs1, errs2)
}

// Close stops polling the queue immediately and waits for all tasks to complete
// before stopping the heartbeat.
func (w *DefaultWorker) Close() {
	w.startClosing(func() {
		w.state = closing
	})
}

// Halt stops the heartbeat and queue polling goroutines immediately and cancels
// all tasks, marking them as FAILED before returning.
func (w *DefaultWorker) Halt() {
	w.startClosing(func() {
		w.state = halting
		w.lifecycle.AbandonAll()
	})
}

// Starts closing the worker if it was not already closed. Invokes the passed
// function to help in the teardown, and blocks until all tasks are done.
func (w *DefaultWorker) startClosing(fn func()) {
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
