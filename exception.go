package redis

import (
	"github.com/goal-web/contracts"
)

type SubscribeException struct {
	error
	fields contracts.Fields
}

func (s SubscribeException) Error() string {
	return s.error.Error()
}

func (s SubscribeException) Fields() contracts.Fields {
	return s.fields
}
