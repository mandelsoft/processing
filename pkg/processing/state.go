package processing

import (
	"sync"
)

type State = *state

var _ Operation = (State)(nil)

// OperationFunction if a GO function, which can be executed by the
// Scheduler. The function gets aa argument describing
// the technical Operation managed by the scheduler. It can be used
// to identify the operation for actions on the scheduler (like
// blocking the execution of operation) This Operation object
// MUST only be used by the OperationFunction (or better, the Go routine
// used to execute the OperationFunction). It should never be stored
// in any object and shared with other Go routines.
type OperationFunction func(Operation)

type state struct {
	lock      sync.Mutex
	name      string
	self      interface{}
	scheduler Scheduler
	blocker   sync.Mutex

	done    Trigger
	blocked bool

	queue Queue
}

func (s *state) Name() string {
	return s.name
}

func (s *state) Preempt() {
	s._preempt()
	s.blocker.Lock()
}

func (s *state) skip() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.done.Trigger()
}

func (s *state) IsDone() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.done.IsTriggered()
}

func (s *state) IsBlocked() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.blocked
}

func (s *state) _block() {
	s.blocker.Lock()
}

func (s *state) _unblock() {
	s.blocker.Unlock()
}

func (s *state) _removedFromQueue(q Queue) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.queue == q {
		s.queue = nil
	}
}

func (s *state) _addToQueue(q Queue, blocked bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.blocked = blocked
	if s.queue != q {
		if s.queue != nil {
			s.queue.Remove(s)
		}
		s.queue = q
		if q != nil {
			q.Add(s)
		}
	}
}

func (s *state) Block(q Queue, r ReleaseFunction) {
	s.scheduler.block(s, q, r)
}

func (s *state) Unblock() {
	s.scheduler.unblock(s)
}

func (s *state) _preempt() {
	s.scheduler.preempt(s)
}
