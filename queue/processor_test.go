package queue_test

import (
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/mock"
)

type MockProcessor struct {
	mock.Mock
}

func (m *MockProcessor) Push(cnx redis.Conn, key string, payload []byte) error {
	args := m.Called(cnx, key, payload)
	return args.Error(0)
}

func (m *MockProcessor) Pull(cnx redis.Conn, key string) ([]byte, error) {
	args := m.Called(cnx, key)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockProcessor) Concat(cnx redis.Conn, src, dest string) error {
	args := m.Called(cnx, src, dest)
	return args.Error(0)
}
