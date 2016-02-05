package queue

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

type DurableQueue struct {
	dmu  sync.RWMutex
	dest string

	*BaseQueue
}

var _ Queue = new(DurableQueue)

func NewDurableQueue(pool *redis.Pool, source, dest string) *DurableQueue {
	return &DurableQueue{
		dest:      dest,
		BaseQueue: NewBaseQueue(pool, source),
	}
}

func (q *DurableQueue) Pull() (payload []byte, err error) {
	cnx := q.pool.Get()
	defer cnx.Close()

	return q.Processor().PullTo(cnx, q.Source(), q.Dest())
}

func (q *DurableQueue) Dest() string {
	q.dmu.RLock()
	defer q.dmu.RUnlock()

	return q.dest
}

func (q *DurableQueue) SetDest(dest string) {
	q.dmu.Lock()
	defer q.dmu.Unlock()

	q.dest = dest
}
