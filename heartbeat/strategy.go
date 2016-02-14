package heartbeat

import "github.com/garyburd/redigo/redis"

// Strategy is an interface to a type which is responsible for both ticking a
// heart's "pulse," and detecting what items in a particular section of Redis
// are to be considered dead.
type Strategy interface {
	// Touch ticks the item at `location:ID` in Redis.
	Touch(location, ID string, pool *redis.Pool) (err error)

	// Removes an item at `location:ID` in Redis.
	Purge(location, ID string, pool *redis.Pool) (err error)

	// Expired returns an array of strings that represent the expired IDs in
	// a given keyspace as specified by the `location` parameter.
	Expired(location string, pool *redis.Pool) (expired []string, err error)
}
