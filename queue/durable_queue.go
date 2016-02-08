package queue

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

// DurableQueue is an implementation of the Queue interface which takes items
// from a source queue and pushes them into the destination queue when Pull() is
// called.
type DurableQueue struct {
	// dmu is a sync.RWMutex that guards the destination string
	dmu sync.RWMutex
	// dest is the Redis keyspace where Pulled() items end up.
	dest string

	// DurableQueue extends a BaseQueue
	BaseQueue
}

// DurableQueue implements the Queue type.
var _ Queue = new(DurableQueue)

// NewDurableQueue initializes and returns a new pointer to an instance of a
// DurableQueue. It is initialized with the given Redis pool, and the source and
// destination queues. By default the FIFO tactic is used, but a call to
// SetProcessor can change this in a safe fashion.
//
// DurableQueues own no goroutines, so this method does not spwawn any
// goroutines or channels.
func NewDurableQueue(pool *redis.Pool, source, dest string) *DurableQueue {
	return &DurableQueue{
		dest:      dest,
		BaseQueue: BaseQueue{source: source, pool: pool},
	}
}

// Pull implements the Pull function on the Queue interface. Unlike common
// implementations of the Queue type, it mutates the Redis keyspace twice, by
// removing an item from one LIST and popping it onto another. It does so by
// delegating into the processor, thus blocking until the processor returns.
func (q *DurableQueue) Pull() (payload []byte, err error) {
	cnx := q.pool.Get()
	defer cnx.Close()

	return q.Processor().PullTo(cnx, q.Source(), q.Dest())
}

// Dest returns the destination keyspace in Redis where pulled items end up. It
// first obtains a read-level lock on the member `dest` variable before
// returning.
func (q *DurableQueue) Dest() string {
	q.dmu.RLock()
	defer q.dmu.RUnlock()

	return q.dest
}

// SetDest updates the destination where items are "pulled" to in a safe,
// blocking manner. It does this by first obtaining a write-level lock on the
// internal member variable wherein the destination is stored, updating, and
// then relinquishing the lock.
//
// It returns the new destination that was just set.
func (q *DurableQueue) SetDest(dest string) string {
	q.dmu.Lock()
	defer q.dmu.Unlock()

	q.dest = dest

	return q.dest
}
