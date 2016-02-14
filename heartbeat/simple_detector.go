package heartbeat

import "github.com/garyburd/redigo/redis"

// SimpleDetector is an implementation of the Detector interface which uses the
// provided Strategy in order to determine which items may be considered dead.
type SimpleDetector struct {
	location string
	pool     *redis.Pool
	strategy Strategy
}

// NewDetector initializes and returns a new SimpleDetector instance with the
// given parameters.
func NewDetector(location string, pool *redis.Pool, strategy Strategy) Detector {
	return SimpleDetector{
		location: location,
		pool:     pool,
		strategy: strategy,
	}
}

// Location returns the keyspace in which the detector searches.
func (d SimpleDetector) Location() string { return d.location }

// Strategy returns the strategy that the detector uses to search in the
// keyspace returned by Location().
func (d SimpleDetector) Strategy() Strategy { return d.strategy }

// Detect implements the `func Detect` on the `type Detector interface`. This
// implementation simply delegates into the provided Strategy.
func (d SimpleDetector) Detect() (expired []string, err error) {
	return d.strategy.Expired(d.location, d.pool)
}

// Purge implements the `func Purge` on the `type Detector interface`. This
// implementation simply delegates into the provided Strategy.
func (d SimpleDetector) Purge(id string) (err error) {
	return d.strategy.Purge(id, d.location, d.pool)
}
