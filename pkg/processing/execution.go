package processing

import (
	"sync"
)

// Execution represent the scheduled execution of
// a Go function of type OperationFunction.
type Execution = *execution

type execution struct {
	lock     sync.Mutex
	function OperationFunction
	state    State
}

type ReleaseFunction func()

// Operation is the identity of the execution of an OperationFunction.
// It can be used by the Go routine used to execute the OperationFunction
// control the execution of the operation.
// An Operation object
// MUST only be used by the OperationFunction (or better, by the Go routine
// used to execute the OperationFunction). It should never be stored
// in any object and shared with other Go routines.
type Operation interface {
	Block(Queue, ReleaseFunction)
	Unblock()
	Preempt()
	_unblock()

	_removedFromQueue(q Queue)
	_addToQueue(Queue, bool)
}

func NewExecution(f OperationFunction, s Scheduler, names ...string) Execution {
	return newExecution(f, s, nil, "execution", names...)
}

func newExecution(f OperationFunction, s Scheduler, self interface{}, typ string, names ...string) Execution {
	e := &execution{function: f}
	if self == nil {
		self = e
	}
	e.state = s.new(self, typ, names...)
	return e
}

func (e *execution) Start() Execution {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.function == nil {
		return nil
	}
	e.state.scheduler.start(e.state, e.function)
	e.function = nil
	return e
}

func (e *execution) Wait(o Operation) {
	e.state.done.Wait(o)
}

func (e *execution) IsDone() bool {
	return e.state.IsDone()
}

func (e *execution) RegisterAction(a TriggerAction) {
	e.state.done.RegisterAction(a)
}
