package heartbeat_test

import (
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/mock"
)

type TestStrategy struct {
	mock.Mock
}

func (s *TestStrategy) Touch(location, ID string, pool *redis.Pool) (err error) {
	args := s.Called(location, ID, pool)
	return args.Error(0)
}

func (s *TestStrategy) Purge(location, ID string, pool *redis.Pool) (err error) {
	args := s.Called(location, ID, pool)
	return args.Error(0)
}

func (s *TestStrategy) Expired(location string, pool *redis.Pool) (expired []string, err error) {
	args := s.Called(location, pool)
	return args.Get(0).([]string), args.Error(1)
}
