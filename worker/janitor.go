package worker

import (
	"math/rand"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/garyburd/redigo/redis"
	"github.com/hjr265/redsync.go/redsync"
	"github.com/mixer/redutil/heartbeat"
	"github.com/mixer/redutil/queue"
)

// The Janitor is used to assist in the tear down of dead workers. It can
// be provided to the worker to hook additional functionality that will
// occur when the worker dies.
type Janitor interface {
	// Called when a worker dies after we have acquired a lock and before
	// we start moving the worker's queue back to the main processing
	// queue. Note that if we does before the worker's queue is moved
	// over, this function *can be called multiple times on the
	// same worker*
	//
	// If an error is returned from the function, the queue concatenation
	// will be aborted and we'll release the lock.
	OnPreConcat(cnx redis.Conn, worker string) error

	// Called when a worker dies after we have acquired a lock and finished
	// moving the worker's queue back to the main processing queue. Note
	// that, in the result of a panic or power failure, this function
	// may never be called, and errors resulting from this function
	// will not roll-back the concatenation.
	OnPostConcat(cnx redis.Conn, worker string) error
}

// Base janitor used unless the user provides a replacement.
type nilJanitor struct{}

var _ Janitor = nilJanitor{}

func (n nilJanitor) OnPreConcat(cnx redis.Conn, worker string) error  { return nil }
func (n nilJanitor) OnPostConcat(cnx redis.Conn, worker string) error { return nil }

// The janitor is responsible for cleaning up dead workers.
type janitorRunner struct {
	// pool is a *redis.Pool used to maintain and use connections into
	// Redis.
	pool *redis.Pool

	availableTasks queue.Queue

	// Associated heartbeater detector
	detector heartbeat.Detector
	janitor  Janitor

	// Duration between dead checks. The first check will come at a time
	// between 0 and time.Duration, so that workers started at the same time
	// don't try to contest the same locks.
	interval time.Duration

	// interval checker used to prune dead workers
	clock clock.Clock

	errs   chan error
	closer chan struct{}
}

func newJanitorRunner(pool *redis.Pool, detector heartbeat.Detector, janitor Janitor,
	availableTasks queue.Queue) *janitorRunner {

	return &janitorRunner{
		pool:           pool,
		availableTasks: availableTasks,
		detector:       detector,
		janitor:        janitor,
		clock:          clock.New(),
		interval:       defaultMonitorInterval,
		errs:           make(chan error),
		closer:         make(chan struct{}),
	}
}

func (j *janitorRunner) watchDead() {
	defer close(j.errs)

	// Sleep for a random interval so that janitors started at the same time
	// don't try to contest the same locks.
	select {
	case <-j.closer:
		return
	case <-j.clock.After(time.Duration(float64(j.interval) * rand.Float64())):
	}

	ticker := j.clock.Ticker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-j.closer:
			return
		case <-ticker.C:
			j.runCleaning()
		}
	}
}

// Detects expired records and starts tasks to move any of their abandoned
// tasks back to the main queue.
func (j *janitorRunner) runCleaning() {
	dead, err := j.detector.Detect()
	if err != nil {
		j.errs <- err
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(dead))

	for _, worker := range dead {
		go func(worker string) {
			defer wg.Done()

			err := j.handleDeath(worker)
			if err != nil && err != redsync.ErrFailed {
				j.errs <- err
			}
		}(worker)
	}

	wg.Wait()
}

// Creates a mutex and attempts to acquire a redlock to dispose of the worker.
func (j *janitorRunner) getLock(worker string) (*redsync.Mutex, error) {
	mu, err := redsync.NewMutexWithPool("redutil:lock:"+worker, []*redis.Pool{j.pool})
	if err != nil {
		return nil, err
	}

	return mu, mu.Lock()
}

// Processes a dead worker, moving its queue back to the main queue and
// calling the disposer function if we get a lock on it.
func (j *janitorRunner) handleDeath(worker string) error {
	mu, err := j.getLock(worker)
	if err != nil {
		return err
	}
	defer mu.Unlock()

	cnx := j.pool.Get()
	defer cnx.Close()

	if err := j.janitor.OnPreConcat(cnx, worker); err != nil {
		return err
	}

	_, err = j.availableTasks.Concat(
		getWorkingQueueName(j.availableTasks.Source(), worker))
	if err != nil && err != redis.ErrNil {
		return err
	}
	j.detector.Purge(worker)

	return j.janitor.OnPostConcat(cnx, worker)
}

func (j *janitorRunner) Close() {
	j.closer <- struct{}{}
}

func (j *janitorRunner) Start() <-chan error {
	j.errs = make(chan error)
	j.closer = make(chan struct{})

	go j.watchDead()

	return j.errs
}
