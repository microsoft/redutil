package queue

import "github.com/garyburd/redigo/redis"

// Processor is an interface to a type encapsulating the interaction between a
// queue.ByteQueue and a datastructure in Redis.
type Processor interface {
	// Push pushes a given `payload` into the keyspace at `key` over the
	// given `redis.Conn`. This function should block until the item can
	// succesfully be confirmed to have been pushed.
	Push(conn redis.Conn, src string, payload []byte) (err error)

	// Pull pulls a given `payload` from the keyspace at `key` over the
	// given `redis.Conn`. This function should block until a timeout has
	// elapsed, or an item is available.
	Pull(conn redis.Conn, src string) (payload []byte, err error)

	// PullTo transfers a given payload from the source (src) keyspace to
	// the destination (dest) keyspace and returns the moved item in the
	// payload space. If an error was encountered, then it will be returned
	// immediately.
	PullTo(conn redis.Conn, src, dest string) (payload []byte, err error)

	// Moves all elements from the src queue to the end of the destination
	// It should return a redis.ErrNil when the source queue is empty.
	Concat(conn redis.Conn, src, dest string) (err error)
}
