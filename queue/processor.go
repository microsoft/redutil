package queue

import (
	"math"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Processor is an interface to a type encapsulating the interaction between a
// queue.ByteQueue and a datastructure in Redis.
type Processor interface {
	// Push pushes a given `payload` into the keyspace at `key` over the
	// given `redis.Conn`. This function should block until the item can
	// succesfully be confirmed to have been pushed.
	Push(conn redis.Conn, src string, payload []byte) (err error)

	// Pull pulls a given `payload` from the keyspace at `key` over the
	// given `redis.Conn`. This function should block until the given
	// timeout has elapsed, or an item is available. If the timeout has
	// passed, a redis.ErrNil will be returned.
	Pull(conn redis.Conn, src string, timeout time.Duration) (payload []byte, err error)

	// PullTo transfers a given payload from the source (src) keyspace to
	// the destination (dest) keyspace and returns the moved item in the
	// payload space. If an error was encountered, then it will be returned
	// immediately. Timeout semantisc are idential to those on Pull, unless
	// noted otherwise in implementation.
	PullTo(conn redis.Conn, src, dest string, timeout time.Duration) (payload []byte, err error)

	// Moves all elements from the src queue to the end of the destination
	// It should return a redis.ErrNil when the source queue is empty.
	Concat(conn redis.Conn, src, dest string) (err error)
}

func block(timeout time.Duration) float64 {
	return math.Ceil(timeout.Seconds())
}
