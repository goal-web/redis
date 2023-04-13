package redis

import (
	"github.com/goal-web/contracts"
	"sync"
)

var (
	// factory 默认 redis 客户端
	factory contracts.RedisFactory
	cli     contracts.RedisConnection
)

// Default 用于不方便注入的地方使用，请确保已经注册了 redis 服务
func Default() contracts.RedisConnection {
	if cli == nil {
		cli = factory.Connection()
	}
	return cli
}

// DefaultFactory 用于不方便注入的地方使用，请确保已经注册了 redis 服务
func DefaultFactory() contracts.RedisFactory {
	return factory
}

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
