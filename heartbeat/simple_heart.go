package heartbeat

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// SimpleHeart is a implementation of the `type Heart interface` which uses a
// given ID, Location and Interval, to call the provided Strategy in order to
// tick items in Redis.
type SimpleHeart struct {
	ID       string
	Location string
	Interval time.Duration
	strategy Strategy

	pool   *redis.Pool
	closer chan struct{}
	errs   chan error
}

// New allocates and returns a new instance of a SimpleHeart, initialized with
// the given parameters.
//
// pool is the *redis.Pool of which the Heart will take connections from.
//
// location is the location in the Redis keyspace wherein the heartbeat will be
// stored. Similarily, `id` is the ID of the given Heart that will be touched in
// that location.
//
// strategy is the Strategy that will be used to Touch() the keyspace in Redis
// at the given interval.
//
// Both the closer and errs channel are initialized to new, empty channels using
// the builtin `make()`.
//
// It begins ticking immediately.
func NewSimpleHeart(id, location string, interval time.Duration, pool *redis.Pool,
	strategy Strategy) SimpleHeart {

	sh := SimpleHeart{
		ID:       id,
		Location: location,
		Interval: interval,

		pool: pool,

		closer:   make(chan struct{}),
		errs:     make(chan error, 1),
		strategy: strategy,
	}

	go sh.heartbeat()
	sh.touch()

	return sh
}

// Close implements the `func Close` defined in the `type Heart interface`. It
// stops the heartbeat process at the next tick.
func (s SimpleHeart) Close() {
	s.closer <- struct{}{}
}

// Errs implements the `func Errs` defined in the `type Heart interface`. It
// returns a read-only channel of errors encountered during the heartbeat
// operation.
func (s SimpleHeart) Errs() <-chan error {
	return s.errs
}

// touch calls .Touch() on the Heart's strategy and pushes any error that
// occurred to the errs channel.
func (s SimpleHeart) touch() {
	if err := s.strategy.Touch(s.Location, s.ID, s.pool); err != nil {
		s.errs <- err
	}
}

// heartbeat is a function responsible for ticking the updater.
//
// It uses a `select` statement to either gather an update from the time.Ticker
// or a close operation. When the Updater is called, the `now` time is passed
// from whatever was on the `ticker.C` channel (`<-chan time.Time`).
//
// If any error occurs during operation, it will be masqueraded up to the
// `s.errs` channel, accessible by the `Errs()` function.
//
// It runs in its own goroutine.
func (s *SimpleHeart) heartbeat() {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.touch()
		case <-s.closer:
			return
		}
	}
}
