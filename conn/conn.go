package conn

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
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
	// Whether or not to secure the connection with TLS
	UseTLS bool
	// Whether to use clustering (redisc)
	UseCluster bool
}

// RedUtilPool is used as a generic for redis.Pool and redisc.Cluster
type RedUtilPool interface {
	Get() redis.Conn
}

// NewWithActiveLimit makes and returns a pointer to a new Connector instance. It sets some
// defaults on the ConnectionParam object, such as the policy, which defaults to
// a LogReconnectPolicy with a base of 10ms. A call to this function does not
// produce a connection.
func NewWithActiveLimit(param ConnectionParam, maxIdle int, maxActive int) (RedUtilPool, ReconnectPolicy) {
	if param.Policy == nil {
		param.Policy = &LogReconnectPolicy{Base: 10, Factor: time.Millisecond}
	}

	options := make([]redis.DialOption, 0)
	if param.UseTLS {
		options = append(options, redis.DialUseTLS(param.UseTLS))
	}

	if param.Password != "" {
		options = append(options, redis.DialPassword(param.Password))
	}

	if param.Timeout > 0 {
		options = append(options, []redis.DialOption{
			redis.DialConnectTimeout(param.Timeout),
			redis.DialReadTimeout(param.Timeout),
			redis.DialWriteTimeout(param.Timeout),
		}...)
	}

	if param.UseCluster {
		return &redisc.Cluster{
			StartupNodes: []string{param.Address},
			DialOptions:  options,
			CreatePool:   connectPool(maxIdle, maxActive),
		}, param.Policy
	}

	return &redis.Pool{Dial: connect(param.Address, options...), MaxIdle: maxIdle, MaxActive: maxActive}, param.Policy
}

// New makes and returns a pointer to a new Connector instance. It sets some
// defaults on the ConnectionParam object, such as the policy, which defaults to
// a LogReconnectPolicy with a base of 10ms. A call to this function does not
// produce a connection.
func New(param ConnectionParam, maxIdle int) (RedUtilPool, ReconnectPolicy) {
	return NewWithActiveLimit(param, maxIdle, 0)
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
func connect(address string, options ...redis.DialOption) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
		return redis.Dial("tcp", address, options...)
	}
}

func connectPool(maxIdle int, maxActive int) func(string, ...redis.DialOption) (*redis.Pool, error) {
	return func(address string, options ...redis.DialOption) (*redis.Pool, error) {
		return &redis.Pool{Dial: connect(address, options...), MaxIdle: maxIdle, MaxActive: maxActive}, nil
	}
}
