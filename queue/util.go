package queue

import "github.com/garyburd/redigo/redis"

// Concat implementation using RPOPLPUSH, compatible
// with the behaviour of Processor.Queue.
func rlConcat(cnx redis.Conn, src, dest string) error {
	data, err := cnx.Do("RPOPLPUSH", src, dest)
	if err != nil {
		return err
	}
	if data == nil {
		return redis.ErrNil
	}

	return nil
}
