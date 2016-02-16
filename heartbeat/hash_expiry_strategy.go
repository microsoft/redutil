package heartbeat

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	// DefaultTimeFormat is a time format string according to the time
	// package and is used to marshal and unmarshall time.Time instances
	// into ISO8601-compliant strings.
	//
	// See: https://www.ietf.org/rfc/rfc3339.txt for more details.
	DefaultTimeFormat string = "2006-01-02T15:04:05"
)

// HashExpireyStrategy is an implementation of Strategy that stores items in a
// hash.
type HashExpireyStrategy struct {
	MaxAge time.Duration
}

var _ Strategy = &HashExpireyStrategy{}

// Touch implements the `func Touch` defined in the Strategy interface. It
// assumes a HASH type is used in Redis to map the IDs of various Hearts to the
// last time that they were updated.
//
// It uses the Heart's `Location` and `ID` fields respectively to determine
// where to both place, and name the hash as well as the items within it.
//
// Times are marshalled using the `const DefaultTimeFormat` which stores times
// in the ISO8601 format.
func (s HashExpireyStrategy) Touch(location, ID string, pool *redis.Pool) error {
	now := time.Now().UTC().Format(DefaultTimeFormat)

	cnx := pool.Get()
	defer cnx.Close()

	if _, err := cnx.Do("HSET", location, ID, now); err != nil {
		return err
	}

	return nil
}

// Purge implements the `func Purge` defined in the Strategy interface. It
// assumes a HASH type is used in Redis to map the IDs of various Hearts,
// and removes the record for the specified ID from the hash.
func (s HashExpireyStrategy) Purge(location, ID string, pool *redis.Pool) error {
	cnx := pool.Get()
	defer cnx.Close()

	if _, err := cnx.Do("HDEL", location, ID); err != nil {
		return err
	}

	return nil
}

// Expired implements the `func Expired` defined on the Strategy interface. It
// scans iteratively over the Heart's `location` field to look for items that
// have expired. An item is marked as expired iff the last update time happened
// before the instant of the maxAge subtracted from the current time.
func (s HashExpireyStrategy) Expired(location string,
	pool *redis.Pool) (expired []string, err error) {

	now := time.Now().UTC()

	cnx := pool.Get()
	defer cnx.Close()

	reply, err := redis.StringMap(cnx.Do("HGETALL", location))
	if err != nil {
		return
	}

	for id, tick := range reply {
		lastUpdate, err := time.Parse(DefaultTimeFormat, tick)

		if err != nil {
			continue
		} else if lastUpdate.Add(s.MaxAge).Before(now) {
			expired = append(expired, id)
		}
	}

	return
}
