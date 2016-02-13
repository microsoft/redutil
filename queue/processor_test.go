package queue_test

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/mock"
)

type MockProcessor struct {
	mock.Mock
}

func (m *MockProcessor) Push(cnx redis.Conn, src string, payload []byte) error {
	args := m.Called(cnx, src, payload)
	return args.Error(0)
}

func (m *MockProcessor) Pull(cnx redis.Conn, src string,
	timeout time.Duration) ([]byte, error) {

	args := m.Called(cnx, src, timeout)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockProcessor) PullTo(cnx redis.Conn, src, dest string,
	timeout time.Duration) ([]byte, error) {

	args := m.Called(cnx, src, dest, timeout)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockProcessor) Concat(cnx redis.Conn, src, dest string) error {
	args := m.Called(cnx, src, dest)
	return args.Error(0)
}
