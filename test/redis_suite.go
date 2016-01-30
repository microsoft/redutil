package test

import (
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/suite"
)

// RedisSuite is a type to be used during testing that wraps the testify's
// `suite.Suite` type and gives a *redis.Pool for us to work with as well.
type RedisSuite struct {
	// Pool is the pool that Redis connections should be pulled from during
	// test.
	Pool *redis.Pool

	suite.Suite
}

// NewSuite constructs a suite with the give pool.
func NewSuite(pool *redis.Pool) *RedisSuite {
	return &RedisSuite{Pool: pool}
}

// SetupTest implements the SetupTest function and entirely clears Redis of
// items before each test run to prevent order-related test issues.
func (s *RedisSuite) SetupTest() {
	s.WithRedis(func(cnx redis.Conn) {
		cnx.Do("FLUSHALL")
	})
}

// WithRedis runs a function and passes it a valid redis.Conn instance. It does
// so by obtaining the redis.Conn instance from the owned *redis.Pool and then
// closing once the outer function has returned.
func (s *RedisSuite) WithRedis(fn func(redis.Conn)) {
	cnx := s.Pool.Get()
	defer cnx.Close()

	fn(cnx)
}
