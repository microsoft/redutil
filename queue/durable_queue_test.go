package queue_test

import (
	"testing"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/test"
	"github.com/stretchr/testify/suite"
)

type DurableQueueSuite struct {
	*test.RedisSuite
}

func TestDurableQueueSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &DurableQueueSuite{test.NewSuite(pool)})
}
