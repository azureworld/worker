package api

import (
	"context"
	"github.com/azureworld/worker/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() interface{}
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	GetDelayedTasks() ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
}

type BrokerBuilder interface {
	Schema() string
	Build(url string) (Broker, error)
}