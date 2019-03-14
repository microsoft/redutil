package queue

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type fifoProcessor struct{}

// FIFO is a first in, first out implementation of the Processor interface.
var FIFO Processor = &fifoProcessor{}

// Push implements the `func Push` from `Processor`. It pushes to the left-side
// of the Redis structure using RPUSH, and returns any errors encountered while
// runnning that command.
func (f *fifoProcessor) Push(cnx redis.Conn, src string, payload []byte) (err error) {
	_, err = cnx.Do("LPUSH", src, payload)
	return
}

// Pull implements the `func Pull` from `Processor`. It pulls from the right-side
// of the Redis structure in a blocking-fashion, using BLPOP.
//
// If an redis.ErrNil is returned, it is silenced, and both fields are returend
// as nil. If the err is not a redis.ErrNil, but is still non-nil itself, then
// it will be returend, along with an empty []byte.
//
// If an item can sucessfully be removed from the keyspace, it is returned
// without error.
func (f *fifoProcessor) Pull(cnx redis.Conn, src string,
	timeout time.Duration) ([]byte, error) {

	slices, err := redis.ByteSlices(cnx.Do("BRPOP", src, block(timeout)))
	if err != nil {
		return nil, err
	}

	return slices[1], nil
}

// PullTo implements the `func PullTo` from the `Processor` interface. It pulls
// from the right-side of the Redis source (src) structure, and pushes to the
// right side of the Redis destination (dest) structure.
func (f *fifoProcessor) PullTo(cnx redis.Conn, src, dest string,
	timeout time.Duration) ([]byte, error) {

	bytes, err := redis.Bytes(cnx.Do("BRPOPLPUSH", src, dest, block(timeout)))
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

// Concat removes the first element from the source list and adds it to the end
// of the destination list. ErrNil is returns when the source is empty.
func (f *fifoProcessor) Concat(cnx redis.Conn, src, dest string) (err error) {
	return rlConcat(cnx, src, dest)
}
