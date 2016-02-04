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
	*BaseQueue
}

// NewByteQueue allocates and returns a pointer to a new instance of a
// ByteQueue. It initializes itself using the given *redis.Pool, and the name,
// which refers to the keyspace wherein these values will be stored.
//
// Internal channels are also initialized here.
func NewByteQueue(pool *redis.Pool, name string) *ByteQueue {
	return &ByteQueue{&BaseQueue{
		source: name,
		pool:   pool,
	}}
}

// Takes all elements from the source queue and adds them to this one. This
// can be a long-running operation. If a persistent error is returned while
// moving things, then it will be returned and the concat will stop, though
// the concat operation can be safely resumed at any time.
func (b *ByteQueue) Concat(src string) error {
	cnx := b.pool.Get()
	defer cnx.Close()

	var err error
	errCount := 0
	for {
		err = b.Processor().Concat(cnx, src, b.Source())
		if err == nil {
			errCount = 0
			continue
		}

		// ErrNil is returned when there are no more items to concat
		if err == redis.ErrNil {
			return nil
		}

		// Command error are bad; something is wrong in db and we should
		// return the problem to the caller.
		if _, cmdErr := err.(redis.Error); cmdErr {
			return err
		}

		// Otherwise this is probably some temporary network error. Close
		// the old connection and try getting a new one.
		errCount++
		if errCount >= concatRetries {
			return err
		}

		cnx.Close()
		cnx = b.pool.Get()
	}
}
