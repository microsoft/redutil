package queue

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

const (
	lpoprpushSrc string = `
local v = redis.call('lpop', KEYS[1])
if v == nil then
	return nil
end

redis.call('rpush', KEYS[2], v)
return v
`
)

var (
	lmu       sync.Mutex
	lpoprpush *redis.Script
)

func LPOPRPUSH(cnx redis.Conn) *redis.Script {
	lmu.Lock()
	defer lmu.Unlock()

	if lpoprpush == nil {
		lpoprpush = redis.NewScript(2, lpoprpushSrc)
	}

	return lpoprpush
}
