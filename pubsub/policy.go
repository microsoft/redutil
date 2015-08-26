package pubsub

import (
	"math"
	"time"
)

// The ReconnectPolicy defines the pattern of delay times used
// after a connection is lost before waiting to reconnection.
type ReconnectPolicy interface {
	// Gets the next reconnect time, usually incrementing some
	// counter so the next attempt is longer.
	Next() time.Duration
	// Resets the number of attempts.
	Reset()
}

// Reconnect policy which waits a set period of time on each connect.
type StaticReconnectPolicy struct {
	// Delay each time.
	Delay time.Duration
}

var _ ReconnectPolicy = new(StaticReconnectPolicy)

func (s *StaticReconnectPolicy) Next() time.Duration {
	return s.Delay
}

func (r *StaticReconnectPolicy) Reset() {}

// Reconnect policy which increases the reconnect day in a logarithmic
// fashion. The equation used is delay = log(tries) / log(base) * Factor
type LogReconnectPolicy struct {
	// Base for the logarithim
	Base float64
	// Duration multiplier for the calculated value.
	Factor time.Duration
	tries  float64
}

var _ ReconnectPolicy = new(LogReconnectPolicy)

func (l *LogReconnectPolicy) Next() time.Duration {
	l.tries += 1
	return time.Duration(math.Log(float64(l.tries)) / math.Log(l.Base) * float64(time.Millisecond))
}

func (l *LogReconnectPolicy) Reset() {
	l.tries = 0
}
