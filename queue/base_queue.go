package queue

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// BaseQueue provides a basic implementation of the Queue interface. Its basic
// methodology is to preform updates using a Processor interface which in and of
// itself defines how updates can be handled.
type BaseQueue struct {
	pool   *redis.Pool
	source string

	pmu       sync.RWMutex
	processor Processor
}

var _ Queue = new(BaseQueue)

func NewBaseQueue(pool *redis.Pool, source string) *BaseQueue {
	return &BaseQueue{
		pool:   pool,
		source: source,
	}
}

// Source implements the Source method on the Queue interface.
func (q *BaseQueue) Source() string {
	return q.source
}

// Push pushes the given payload (a byte slice) into the specified keyspace by
// delegating into the `Processor`'s `func Push`. It obtains a connection to
// Redis using the pool, which is passed into the Processor, and recycles that
// connection after the function has returned.
//
// If an error occurs during Pushing, it will be returned, and it can be assumed
// that the payload is not in Redis.
func (q *BaseQueue) Push(payload []byte) (err error) {
	cnx := q.pool.Get()
	defer cnx.Close()

	return q.Processor().Push(cnx, q.Source(), payload)
}

// Source implements the Source method on the Queue interface.
func (q *BaseQueue) Pull(timeout time.Duration) (payload []byte, err error) {
	cnx := q.pool.Get()
	defer cnx.Close()

	return q.Processor().Pull(cnx, q.Source(), timeout)
}

// Source implements the Source method on the Queue interface. It functions by
// requesting a read-level lock from the guarding mutex and returning that value
// once obtained. If no processor is set, the the default FIFO implementation is
// returned.
func (q *BaseQueue) Processor() Processor {
	q.pmu.RLock()
	defer q.pmu.RUnlock()

	if q.processor == nil {
		return FIFO
	}

	return q.processor
}

// SetProcessor implements the SetProcessor method on the Queue interface. It
// functions by requesting write-level access from the guarding mutex and
// preforms the update atomically.
func (q *BaseQueue) SetProcessor(processor Processor) {
	q.pmu.Lock()
	defer q.pmu.Unlock()

	q.processor = processor
}

// Takes all elements from the source queue and adds them to this one. This
// can be a long-running operation. If a persistent error is returned while
// moving things, then it will be returned and the concat will stop, though
// the concat operation can be safely resumed at any time.
func (q *BaseQueue) Concat(src string) (moved int, err error) {
	cnx := q.pool.Get()
	defer cnx.Close()

	errCount := 0
	for {
		err = q.Processor().Concat(cnx, src, q.Source())
		if err == nil {
			errCount = 0
			moved++
			continue
		}

		// ErrNil is returned when there are no more items to concat
		if err == redis.ErrNil {
			return
		}

		// Command error are bad; something is wrong in db and we should
		// return the problem to the caller.
		if _, cmdErr := err.(redis.Error); cmdErr {
			return
		}

		// Otherwise this is probably some temporary network error. Close
		// the old connection and try getting a new one.
		errCount++
		if errCount >= concatRetries {
			return
		}

		cnx.Close()
		cnx = q.pool.Get()
	}
}
