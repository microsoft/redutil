package queue

import "github.com/garyburd/redigo/redis"

// The number of errors we can get while concatting in a row before giving
// up and just returning.
const concatRetries int = 3

// ByteQueue represents either a FILO or FIFO queue contained in a particular
// Redis keyspace. It allows callers to push `[]byte` payloads, and receive them
// back over the `In() <-chan []byte`. It is typically used in a distributed
// setting, where the pusher may not always get the item back.
type ByteQueue struct {
	BaseQueue
}

// NewByteQueue allocates and returns a pointer to a new instance of a
// ByteQueue. It initializes itself using the given *redis.Pool, and the name,
// which refers to the keyspace wherein these values will be stored.
//
// Internal channels are also initialized here.
func NewByteQueue(pool *redis.Pool, name string) *ByteQueue {
	return &ByteQueue{BaseQueue{
		source: name,
		pool:   pool,
	}}
}
