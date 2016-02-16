package worker

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/WatchBeam/redutil/queue"
	"github.com/garyburd/redigo/redis"
)

// ErrNotFound is returned if you attempt to mark a task as complete or abandoned
// that wasn't registered in the lifecycle.
var ErrNotFound = errors.New("Attempted to complete a task that we aren't working on.")

// deleteToken is the value deleted items are set to pending removal: 16
// random bytes. This is used since removal of items in Redis is necessarily
// a two-step operation; we can only delete items by value, not by index in
// the queue, but we *can* set items by their index.
//
// Hopefully Redis, at some point, implements removal by index, allowing
// us to do away with this ugliness.
var deleteToken = []byte{0x16, 0xea, 0x58, 0x1f, 0xbd, 0x4a, 0x23, 0xc2,
	0x66, 0x97, 0x8a, 0x35, 0xb7, 0xd0, 0x22, 0xef}

// DefaultLifecycle provides a default implementation of the Lifecycle
// interface. It moves tasks from a source, which provides available tasks, into
// a specific worker queue, which is the list of items that this worker is
// currently working on.
//
// Completed tasks leave the individualized worker queue, while abandoned tasks
// move back to the task source.
type DefaultLifecycle struct {
	// pool is a *redis.Pool used to maintain and use connections into
	// Redis.
	pool *redis.Pool

	availableTasks queue.Queue
	workingTasks   *queue.DurableQueue

	// rmu guards registry
	rmu sync.Mutex
	// registry is a local copy of the workingTasks queue so we can easily
	// delete items by index.
	registry []*Task

	// tasks are a channel of tasks that this worker has taken ownership of
	// and needs to work on.
	tasks chan *Task
	// errs is a channel of errors that gets written to when an error is
	// encountered by the recv() function.
	errs chan error

	// closer is a channel that receives an empty message when it is time to
	// close.
	closer chan struct{}

	// wg is a WaitGroup that keeps track of all actively owned tasks. When
	// a task is pulled, the state of this WaitGroup increases, and
	// conversely decreases when the task is either COMPLETED, or FAILED.
	wg sync.WaitGroup
}

// NewLifecycle allocates and returns a pointer to a new instance of
// DefaultLifecycle. It uses the specified pool to make connections into Redis
// and a queue of available tasks along with a second working tasks queue,
// which stores the items the lifecycle is currently processing.
func NewLifecycle(pool *redis.Pool) *DefaultLifecycle {
	return &DefaultLifecycle{pool: pool}
}

var _ Lifecycle = new(DefaultLifecycle)

func (l *DefaultLifecycle) Await() {
	l.wg.Wait()
}

func (l *DefaultLifecycle) SetQueues(availableTasks queue.Queue,
	workingTasks *queue.DurableQueue) {
	l.availableTasks = availableTasks
	l.workingTasks = workingTasks
}

// Listen returns a channel of tasks and error that are pulled from the
// main processing queue. Once a *Task is able to be read from the <-chan *Task,
// that Task is ready to be worked on and is in the appropriate locations in
// Redis. StopListening() can be called to terminate.
func (l *DefaultLifecycle) Listen() (<-chan *Task, <-chan error) {
	l.tasks = make(chan *Task)
	l.errs = make(chan error)
	l.closer = make(chan struct{})

	go l.recv()

	return l.tasks, l.errs
}

// StopListening closes the queue "pull" task at the next opportunity. Tasks
// can still be marked as completed or abandoned, but no new tasks will
// be generated.
func (l *DefaultLifecycle) StopListening() {
	l.closer <- struct{}{}
}

// Complete marks a task as having been completed, removing it from the
// worker's queue.
func (l *DefaultLifecycle) Complete(task *Task) error {
	l.removeTask(task)

	return nil
}

// Abandon marks a task as having failed, pushing it back onto the primary
// task queue and removing it from our worker queue.
func (l *DefaultLifecycle) Abandon(task *Task) error {
	if err := l.availableTasks.Push(task.Bytes()); err != nil {
		return err
	}

	return l.removeTask(task)
}

// Marks *all* tasks in the queue as having been abandoned. Called by the
// worker in the Halt() method.
func (l *DefaultLifecycle) AbandonAll() error {
	l.rmu.Lock()
	defer l.rmu.Unlock()

	moved, err := l.availableTasks.Concat(l.workingTasks.Dest())
	remaining := len(l.registry) - moved

	l.registry = l.registry[0:remaining]
	l.wg.Add(-1 * remaining)

	return err
}

// recv pulls items from the queue and publishes them into the <-chan *Task. If
// a message is sent over the `closer` channel, then this process will be
// stoped.
func (l *DefaultLifecycle) recv() {
	for {
		select {
		case <-l.closer:
			close(l.errs)
			return
		default:
			payload, err := l.workingTasks.Pull(time.Second)
			if err == redis.ErrNil {
				continue
			}

			if err != nil {
				l.errs <- err
				continue
			}

			if payload == nil || bytes.Equal(deleteToken, payload) {
				continue
			}

			task := NewTask(l, payload)
			l.addTask(task)
			l.tasks <- task
		}
	}
}

// addTask inserts a newly created task into the internal tasks registry.
func (l *DefaultLifecycle) addTask(t *Task) {
	l.rmu.Lock()
	defer l.rmu.Unlock()

	l.registry = append([]*Task{t}, l.registry...)
	l.wg.Add(1)
}

// Removes a task from the worker's task queue.
func (l *DefaultLifecycle) removeTask(task *Task) (err error) {
	l.rmu.Lock()
	defer l.rmu.Unlock()
	cnx := l.pool.Get()
	defer cnx.Close()
	defer l.wg.Done()

	i := l.findTaskIndex(task)
	if i == -1 {
		return ErrNotFound
	}

	count := len(l.registry)
	l.registry = append(l.registry[:i], l.registry[i+1:]...)

	// We set the item relative to the end position of the list. Since the
	// queue is running BRPOPLPUSH, the index relative to the start of the
	// list (left side) might change in the meantime.
	_, err = cnx.Do("LSET", l.workingTasks.Dest(), i-count, deleteToken)
	if err != nil {
		return
	}

	// Ignore errors from trimming. If this fails, it's unfortunate, but the
	// task was still removed successfully. The next LTRIM will remove the
	// item or, if not, we'll just ignore it if we read it from the queue.
	cnx.Do("LREM", l.workingTasks.Dest(), 0, deleteToken)

	return nil
}

// Returns the index of the task in the tasks list. Returns -1 if the task
// was not in the list.
func (l *DefaultLifecycle) findTaskIndex(task *Task) int {
	for i, t := range l.registry {
		if t == task {
			return i
		}
	}

	return -1
}
