package worker

import (
	"errors"
	"github.com/azureworld/worker/api"
)

var registerBrokerBuilders = make(map[string]api.BrokerBuilder)

var registerBackendBuilders = make(map[string]api.BackendBuilder)

func FetchBroker(schema string, url string) (api.Broker, error) {
	if builder, ok := registerBrokerBuilders[schema]; ok {
		return builder.Build(url)
	}
	return nil, errors.New("no such schema ,may be not import _")
}

func FetchBackend(schema string, url string) (api.Backend, error) {
	if builder, ok := registerBackendBuilders[schema]; ok {
		return builder.Build(url)
	}
	return nil, errors.New("no such schema ,may be not import _")
}
