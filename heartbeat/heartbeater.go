package heartbeat

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// Heartbeater serves as a factory-type, in essence, to create both Hearts and
// Detectors. It maintains information about the ID used to heartbeat with, the
// Location in which to store ticks, the interval at which to update that
// location, the pool in which to maintain and recycle Redis connections to, and
// the strategy to use to tick and recover items in Redis.
type Heartbeater struct {
	// ID is a unique identifier used by the Heart to tick with.
	ID string

	// Location is the absolute path of the keyspace in Redis in which to
	// store all of the heartbeat updates.
	Location string

	// interval is the Interval at which to tell the Heart to tick itself.
	interval time.Duration

	// pool is the *redis.Pool that connections are derived from.
	pool *redis.Pool

	// Strategy is the strategy to use both to tick the items in Redis, but
	// also to determine which ones are still alive.
	Strategy Strategy
}

// New allocates and returns a pointer to a new instance of a Heartbeater. It
// takes in the id, location, interval and pool with which to use to create
// Hearts and Detectors.
func New(id, location string, interval time.Duration, pool *redis.Pool) *Heartbeater {
	return &Heartbeater{
		ID:       id,
		Location: location,
		interval: interval,
		pool:     pool,
	}
}

// Interval returns the interval at which the heart should tick itself.
func (h *Heartbeater) Interval() time.Duration {
	return h.interval
}

// Heart creates and returns a new instance of the Heart type with the
// parameters used by the Heartbeater for consistency.
func (h *Heartbeater) Heart() Heart {
	return NewSimpleHeart(h.ID, h.Location, h.interval, h.pool, h.Strategy)
}

// Detectors creates and returns a new instance of the Detector type with the
// parameters used by the Heartbeater for consistency.
func (h *Heartbeater) Detector() Detector {
	return NewDetector(h.Location, h.pool, h.Strategy)
}

// MaxAge returns the maximum amount of time that can pass from the last tick of
// an item before that item may be considered dead. By default, it is three
// halves of the Interval() time, but is only one second if the interval time is
// less than five seconds.
func (h *Heartbeater) MaxAge() time.Duration {
	if h.Interval() < 5*time.Second {
		return h.Interval() + time.Second
	}

	return h.Interval() * 3 / 2
}

// SetStrategy changes the strategy used by all future Heart and Detector
// instantiations.
func (h *Heartbeater) SetStrategy(strategy Strategy) *Heartbeater {
	h.Strategy = strategy

	return h
}
