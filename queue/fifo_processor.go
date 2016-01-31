package queue

import "github.com/garyburd/redigo/redis"

type fifoProcessor struct{}

// FIFO is a first in, first out implementation of the Processor interface.
var FIFO Processor = &fifoProcessor{}

// Push implements the `func Push` from `Processor`. It pushes the right-side
// of the Redis structure using RPUSH, and returns any errors encountered while
// runnning that command.
func (l *fifoProcessor) Push(cnx redis.Conn, key string, payload []byte) (err error) {
	_, err = cnx.Do("LPUSH", key, payload)
	return
}

// Pull implements the `func Pull` from `Processor`. It pulls from the left-side
// of the Redis structure in a blocking-fashion, using BLPOP. It waits one
// second before timing out.
//
// If an redis.ErrNil is returned, it is silenced, and both fields are returend
// as nil. If the err is not a redis.ErrNil, but is still non-nil itself, then
// it will be returend, along with an empty []byte.
//
// If an item can sucessfully be removed from the keyspace, it is returned
// without error.
func (l *fifoProcessor) Pull(cnx redis.Conn, key string) ([]byte, error) {
	slices, err := redis.ByteSlices(cnx.Do("BRPOP", key, 1))
	if err == redis.ErrNil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return slices[1], nil
}

// Removes the first element from the source list and adds it to the end
// of the destination list. ErrNil is returns when the source is empty.
func (f *fifoProcessor) Concat(cnx redis.Conn, src, dest string) (err error) {
	return rlConcat(cnx, src, dest)
}
