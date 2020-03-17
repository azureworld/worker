package worker

import (
	"errors"
	"fmt"
	"github.com/azureworld/worker/log"
	"github.com/azureworld/worker/retry"
	"github.com/azureworld/worker/tasks"
	"github.com/azureworld/worker/tracing"
	"github.com/opentracing/opentracing-go"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Worker represents a single worker process
type Worker struct {
	manager         *Manager
	ConsumerTag     string
	Concurrency     int
	Queue           string
	errorHandler    func(err error)
	preTaskHandler  func(*tasks.Signature)
	postTaskHandler func(*tasks.Signature)
}

var (
	// ErrWorkerQuitGracefully is return when worker quit gracefully
	ErrWorkerQuitGracefully = errors.New("Worker quit gracefully")
	// ErrWorkerQuitGracefully is return when worker quit abruptly
	ErrWorkerQuitAbruptly = errors.New("Worker quit abruptly")
)

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (w *Worker) Launch() error {
	errorsChan := make(chan error)

	w.LaunchAsync(errorsChan)

	return <-errorsChan
}

// LaunchAsync is a non blocking version of Launch
func (w *Worker) LaunchAsync(errorsChan chan<- error) {
	cnf := w.manager.GetConfig()
	broker := w.manager.GetBroker()

	// Log some useful information about w configuration
	log.INFO.Printf("Launching a w with the following settings:")
	log.INFO.Printf("- Broker: %s", cnf.Broker)
	if w.Queue == "" {
		log.INFO.Printf("- DefaultQueue: %s", cnf.DefaultQueue)
	} else {
		log.INFO.Printf("- CustomQueue: %s", w.Queue)
	}
	log.INFO.Printf("- ResultBackend: %s", cnf.ResultBackend)
	//if cnf.AMQP != nil {
	//	log.INFO.Printf("- AMQP: %s", cnf.AMQP.Exchange)
	//	log.INFO.Printf("  - Exchange: %s", cnf.AMQP.Exchange)
	//	log.INFO.Printf("  - ExchangeType: %s", cnf.AMQP.ExchangeType)
	//	log.INFO.Printf("  - BindingKey: %s", cnf.AMQP.BindingKey)
	//	log.INFO.Printf("  - PrefetchCount: %d", cnf.AMQP.PrefetchCount)
	//}

	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		for {
			retry, err := broker.StartConsuming(w.ConsumerTag, w.Concurrency, w)

			if retry {
				if w.errorHandler != nil {
					w.errorHandler(err)
				} else {
					log.WARNING.Printf("Broker failed with error: %s", err)
				}
			} else {
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()
	if !cnf.NoUnixSignals {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		var signalsReceived uint

		// Goroutine Handle SIGINT and SIGTERM signals
		go func() {
			for {
				select {
				case s := <-sig:
					log.WARNING.Printf("Signal received: %v", s)
					signalsReceived++

					if signalsReceived < 2 {
						// After first Ctrl+C start quitting the w gracefully
						log.WARNING.Print("Waiting for running tasks to finish before shutting down")
						go func() {
							w.Quit()
							errorsChan <- ErrWorkerQuitGracefully
						}()
					} else {
						// Abort the program when user hits Ctrl+C second time in a row
						errorsChan <- ErrWorkerQuitAbruptly
					}
				}
			}
		}()
	}
}

// CustomQueue returns Custom Queue of the running worker process
func (w *Worker) CustomQueue() string {
	return w.Queue
}

// Quit tears down the running worker process
func (w *Worker) Quit() {
	w.manager.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (w *Worker) Process(signature *tasks.Signature) error {
	// If the task is not registered with this w, do not continue
	// but only return nil as we do not want to restart the w process
	if !w.manager.IsTaskRegistered(signature.Name) {
		return nil
	}

	taskFunc, err := w.manager.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	// Update task state to RECEIVED
	if err = w.manager.GetBackend().SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set state to 'received' for task %s returned error: %s", signature.UUID, err)
	}

	// Prepare task for processing
	task, err := tasks.NewWithSignature(taskFunc, signature)
	// if this failed, it means the task is malformed, probably has invalid
	// signature, go directly to task failed without checking whether to retry
	if err != nil {
		w.taskFailed(signature, err)
		return err
	}

	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	taskSpan := tracing.StartSpanFromHeaders(signature.Headers, signature.Name)
	tracing.AnnotateSpanWithSignatureInfo(taskSpan, signature)
	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)

	// Update task state to STARTED
	if err = w.manager.GetBackend().SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set state to 'started' for task %s returned error: %s", signature.UUID, err)
	}

	//Run handler before the task is called
	if w.preTaskHandler != nil {
		w.preTaskHandler(signature)
	}

	//Defer run handler for the end of the task
	if w.postTaskHandler != nil {
		defer w.postTaskHandler(signature)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		// If a tasks.ErrRetryTaskLater was returned from the task,
		// retry the task after specified duration
		retriableErr, ok := interface{}(err).(tasks.ErrRetryTaskLater)
		if ok {
			return w.retryTaskIn(signature, retriableErr.RetryIn())
		}

		// Otherwise, execute default retry logic based on signature.RetryCount
		// and signature.RetryTimeout values
		if signature.RetryCount > 0 {
			return w.taskRetry(signature)
		}

		return w.taskFailed(signature, err)
	}

	return w.taskSucceeded(signature, results)
}

// retryTask decrements RetryCount counter and republishes the task to the queue
func (w *Worker) taskRetry(signature *tasks.Signature) error {
	// Update task state to RETRY
	if err := w.manager.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
	}

	// Decrement the retry counter, when it reaches 0, we won't retry again
	signature.RetryCount--

	// Increase retry timeout
	signature.RetryTimeout = retry.FibonacciNext(signature.RetryTimeout)

	// Delay task by signature.RetryTimeout seconds
	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
	signature.ETA = &eta

	log.WARNING.Printf("Task %s failed. Going to retry in %d seconds.", signature.UUID, signature.RetryTimeout)

	// Send the task back to the queue
	_, err := w.manager.SendTask(signature)
	return err
}

// taskRetryIn republishes the task to the queue with ETA of now + retryIn.Seconds()
func (w *Worker) retryTaskIn(signature *tasks.Signature, retryIn time.Duration) error {
	// Update task state to RETRY
	if err := w.manager.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
	}

	// Delay task by retryIn duration
	eta := time.Now().UTC().Add(retryIn)
	signature.ETA = &eta

	log.WARNING.Printf("Task %s failed. Going to retry in %.0f seconds.", signature.UUID, retryIn.Seconds())

	// Send the task back to the queue
	_, err := w.manager.SendTask(signature)
	return err
}

// taskSucceeded updates the task state and triggers success callbacks or a
// chord callback if this was the last task of a group with a chord callback
func (w *Worker) taskSucceeded(signature *tasks.Signature, taskResults []*tasks.TaskResult) error {
	// Update task state to SUCCESS
	if err := w.manager.GetBackend().SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set state to 'success' for task %s returned error: %s", signature.UUID, err)
	}

	// Log human readable results of the processed task
	var debugResults = "[]"
	results, err := tasks.ReflectTaskResults(taskResults)
	if err != nil {
		log.WARNING.Print(err)
	} else {
		debugResults = tasks.HumanReadableResults(results)
	}
	log.DEBUG.Printf("Processed task %s. Results = %s", signature.UUID, debugResults)

	// Trigger success callbacks

	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			for _, taskResult := range taskResults {
				successTask.Args = append(successTask.Args, tasks.Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}

		w.manager.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := w.manager.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("Completed check for group %s returned error: %s", signature.GroupUUID, err)
	}

	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Defer purging of group meta queue if we are using AMQP backend
	if w.hasAMQPBackend() {
		defer w.manager.GetBackend().PurgeGroupMeta(signature.GroupUUID)
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := w.manager.GetBackend().TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("Triggering chord for group %s returned error: %s", signature.GroupUUID, err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := w.manager.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				signature.ChordCallback.Args = append(signature.ChordCallback.Args, tasks.Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}
	}

	// Send the chord task
	_, err = w.manager.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// taskFailed updates the task state and triggers error callbacks
func (w *Worker) taskFailed(signature *tasks.Signature, taskErr error) error {
	// Update task state to FAILURE
	if err := w.manager.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
		return fmt.Errorf("Set state to 'failure' for task %s returned error: %s", signature.UUID, err)
	}

	if w.errorHandler != nil {
		w.errorHandler(taskErr)
	} else {
		log.ERROR.Printf("Failed processing task %s. Error = %v", signature.UUID, taskErr)
	}

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]tasks.Arg{{
			Type:  "string",
			Value: taskErr.Error(),
		}}, errorTask.Args...)
		errorTask.Args = args
		w.manager.SendTask(errorTask)
	}

	if signature.StopTaskDeletionOnError {
		//return errs.ErrStopTaskDeletion
		return nil
	}

	return nil
}

// Returns true if the worker uses AMQP backend
func (w *Worker) hasAMQPBackend() bool {
	//_, ok := w.manager.GetBackend().(*amqp.Backend)
	//return ok
	return false
}

// SetErrorHandler sets a custom error handler for task errors
// A default behavior is just to log the error after all the retry attempts fail
func (w *Worker) SetErrorHandler(handler func(err error)) {
	w.errorHandler = handler
}

//SetPreTaskHandler sets a custom handler func before a job is started
func (w *Worker) SetPreTaskHandler(handler func(*tasks.Signature)) {
	w.preTaskHandler = handler
}

//SetPostTaskHandler sets a custom handler for the end of a job
func (w *Worker) SetPostTaskHandler(handler func(*tasks.Signature)) {
	w.postTaskHandler = handler
}

//GetServer returns manager
func (w *Worker) GetServer() *Manager {
	return w.manager
}
