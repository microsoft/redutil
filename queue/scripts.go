package queue

import "github.com/garyburd/redigo/redis"

var LPOPRPUSH = redis.NewScript(2, `
	local v = redis.call('lpop', KEYS[1])
	if v == nil or v == false then
		return nil
	end
	
	redis.call('rpush', KEYS[2], v)
	return v
`)
