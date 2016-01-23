package conn

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// Used to denote the parameters of the redis connection.
type ConnectionParam struct {
	// Host:port
	Address string
	// Optional password. Defaults to no authentication.
	Password string
	// Policy to use for reconnections (defaults to
	// LogReconnectPolicy with a base of 10 and factor of 1 ms)
	Policy ReconnectPolicy
	// Dial timeout for redis (defaults to no timeout)
	Timeout time.Duration
}

// New makes and returns a pointer to a new Connector instance. It sets some
// defaults on the ConnectionParam object, such as the policy, which defaults to
// a LogReconnectPolicy with a base of 10ms. A call to this function does not
// produce a connection.
func New(param ConnectionParam, maxIdle int) (*redis.Pool, ReconnectPolicy) {
	if param.Policy == nil {
		param.Policy = &LogReconnectPolicy{Base: 10, Factor: time.Millisecond}
	}

	return redis.NewPool(connect(param), maxIdle), param.Policy
}

// connect is a higher-order function that returns a function that dials,
// connects, and authenticates a Redis connection.
//
// It attempts to dial a TCP connection to the address specified, timing out if
// no connection was able to be established within the given time-frame. If no
// timeout was given, it will wait indefinitely.
//
// If a password as specified in the ConnectionParam object, then an `AUTH`
// command (see: http://redis.io/commands/auth) is issued with that password.
//
// If an error is incurred either dialing the TCP connection, or sending the
// `AUTH` command, then it will be returned immediately, and the client can be
// considered useless.
func connect(param ConnectionParam) func() (redis.Conn, error) {
	return func() (cnx redis.Conn, err error) {
		if param.Timeout > 0 {
			cnx, err = redis.DialTimeout("tcp", param.Address,
				param.Timeout, param.Timeout, param.Timeout)
		} else {
			cnx, err = redis.Dial("tcp", param.Address)
		}

		if err != nil {
			return
		}

		if param.Password != "" {
			if _, err = cnx.Do("AUTH", param.Password); err != nil {
				return
			}
		}

		return
	}
}
