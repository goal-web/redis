package redis

import (
	"sync"

	"github.com/goal-web/application"
	"github.com/goal-web/contracts"
)

var factory contracts.RedisFactory
var once sync.Once

func Default() contracts.RedisFactory {
	once.Do(func() {
		factory = application.Get("redis.factory").(contracts.RedisFactory)
	})

	return factory
}

func Conn(name ...string) contracts.RedisConnection {
	return Default().Connection(name...)
}
