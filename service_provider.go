package redis

import (
	"sync"

	"github.com/goal-web/contracts"
)

type ServiceProvider struct {
}

func NewService() contracts.ServiceProvider {
	return &ServiceProvider{}
}

func (provider ServiceProvider) Stop() {

}

func (provider ServiceProvider) Start() error {
	return nil
}

func (provider ServiceProvider) Register(app contracts.Application) {

	app.Singleton("redis.factory", func(config contracts.Config, handler contracts.ExceptionHandler) contracts.RedisFactory {
		factory = &Factory{
			config:           config.Get("redis").(Config),
			exceptionHandler: handler,
			connections:      make(map[string]contracts.RedisConnection),
			mutex:            sync.Mutex{},
		}

		return factory
	})

	app.Singleton("redis", func(factory contracts.RedisFactory) contracts.RedisConnection {
		return factory.Connection()
	})

	app.Singleton("redis.connection", func(redis contracts.RedisConnection) *Connection {
		return redis.(*Connection)
	})
}
