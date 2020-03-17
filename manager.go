package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/azureworld/worker/api"
	"github.com/azureworld/worker/config"
	"github.com/azureworld/worker/tasks"
	"github.com/azureworld/worker/tracing"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
)

// Manager is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the manager
type Manager struct {
	config            *config.Config
	registeredTasks   map[string]interface{}
	broker            api.Broker
	backend           api.Backend
	prePublishHandler func(*tasks.Signature)
}

// NewManagerWithBrokerBackend ...
func NewManagerWithBrokerBackend(cnf *config.Config, brokerManager api.Broker, backendManager api.Backend) *Manager {
	return &Manager{
		config:          cnf,
		registeredTasks: make(map[string]interface{}),
		broker:          brokerManager,
		backend:         backendManager,
	}
}

// NewManager creates Manager instance
func NewManager(cnf *config.Config) (*Manager, error) {
	broker, err := FetchBroker(cnf.Broker, cnf.Broker)
	if err != nil {
		return nil, err
	}
	backend, _ := FetchBackend(cnf.ResultBackend, cnf.ResultBackend)
	manager := NewManagerWithBrokerBackend(cnf, broker, backend)
	return manager, nil
}

// NewWorker creates Worker instance
func (m *Manager) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		manager:     m,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (m *Manager) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		manager:     m,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (m *Manager) GetBroker() api.Broker {
	return m.broker
}

// SetBroker sets broker
func (m *Manager) SetBroker(broker api.Broker) {
	m.broker = broker
}

// GetBackend returns backend
func (m *Manager) GetBackend() api.Backend {
	return m.backend
}

// SetBackend sets backend
func (m *Manager) SetBackend(backend api.Backend) {
	m.backend = backend
}

// GetConfig returns connection object
func (m *Manager) GetConfig() *config.Config {
	return m.config
}

// SetConfig sets config
func (m *Manager) SetConfig(cnf *config.Config) {
	m.config = cnf
}

// SetPreTaskHandler Sets pre publish handler
func (m *Manager) SetPreTaskHandler(handler func(*tasks.Signature)) {
	m.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (m *Manager) RegisterTasks(namedTasks map[string]interface{}) error {
	for _, task := range namedTasks {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}
	m.registeredTasks = namedTasks
	m.broker.SetRegisteredTaskNames(m.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (m *Manager) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	m.registeredTasks[name] = taskFunc
	m.broker.SetRegisteredTaskNames(m.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (m *Manager) IsTaskRegistered(name string) bool {
	_, ok := m.registeredTasks[name]
	return ok
}

// GetRegisteredTask returns registered task by name
func (m *Manager) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := m.registeredTasks[name]
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %m", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (m *Manager) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	// Make sure result backend is defined
	if m.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := m.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %m", err)
	}

	if m.prePublishHandler != nil {
		m.prePublishHandler(signature)
	}

	if err := m.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %m", err)
	}

	return NewAsyncResult(signature, m.backend), nil
}

// SendTask publishes a task to the default queue
func (m *Manager) SendTask(signature *tasks.Signature) (*AsyncResult, error) {
	return m.SendTaskWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (m *Manager) SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChainTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChainInfo(span, chain)

	return m.SendChain(chain)
}

// SendChain triggers a chain of tasks
func (m *Manager) SendChain(chain *tasks.Chain) (*ChainAsyncResult, error) {
	_, err := m.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return NewChainAsyncResult(chain.Tasks, m.backend), nil
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (m *Manager) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowGroupTag)
	defer span.Finish()

	tracing.AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	// Make sure result backend is defined
	if m.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group
	m.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

	// Init the tasks Pending state first
	for _, signature := range group.Tasks {
		if err := m.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if sendConcurrency > 0 {
			<-pool
		}

		go func(sig *tasks.Signature, index int) {
			defer wg.Done()

			// Publish task

			err := m.broker.Publish(ctx, sig)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %m", err)
				return
			}

			asyncResults[index] = NewAsyncResult(sig, m.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendGroup triggers a group of parallel tasks
func (m *Manager) SendGroup(group *tasks.Group, sendConcurrency int) ([]*AsyncResult, error) {
	return m.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (m *Manager) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChordTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	_, err := m.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		m.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
func (m *Manager) SendChord(chord *tasks.Chord, sendConcurrency int) (*ChordAsyncResult, error) {
	return m.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// GetRegisteredTaskNames returns slice of registered task names
func (m *Manager) GetRegisteredTaskNames() []string {
	taskNames := make([]string, len(m.registeredTasks))
	var i = 0
	for name := range m.registeredTasks {
		taskNames[i] = name
		i++
	}
	return taskNames
}
