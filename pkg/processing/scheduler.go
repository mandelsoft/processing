package processing

import (
	"sync"
)

// Scheduler is able to handle the execution of operations in parallel.
// An operation is the execution of an OperationFunction.
// Hereby, the number of concurrent operations is limited to the
// limit passed to the scheduler constructor.
// There might be any number of operations in progress, but the
// number of actually running operations is limited.
// The scheduler handles this by observing the executions blocked
// on dedicated synchronization primitives supported by this
// package.
type Scheduler = *scheduler

type scheduler struct {
	lock              sync.Mutex
	num_processors    int
	active_processors int

	running Queue
	ready   Queue
	blocked Queue
	bcnt    int
}

func New(n int) Scheduler {
	return &scheduler{
		num_processors: n,

		running: NewQueue("running"),
		ready:   NewQueue("ready"),
		blocked: NewQueue("blocked"),
	}
}

func (s *scheduler) ActiveCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.running.Len() + s.ready.Len()
}

func (s *scheduler) RunningCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.running.Len()
}

func (s *scheduler) ReadyCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.ready.Len()
}

func (s *scheduler) BlockedCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.bcnt
}

func (s *scheduler) WaitingCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.bcnt - s.blocked.Len()
}

func (s *scheduler) new(self interface{}, typ string, names ...string) State {
	s.lock.Lock()
	defer s.lock.Unlock()

	return &state{
		self:      self,
		name:      ElementName(typ, names...),
		scheduler: s,
		done:      NewArmedTrigger(nil),
	}
}

func (s *scheduler) start(b State, f func(o Operation)) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.active_processors < s.num_processors {
		s.active_processors++
		s.running.Add(b)
	} else {
		b._block()
		s.ready.Add(b)
	}
	go func() {
		b.blocker.Lock()
		f(b)
		s.done(b)
	}()
}

func (s *scheduler) done(b State) {
	s.lock.Lock()
	s.running.Remove(b)
	s._schedule()
	s.lock.Unlock()
	b.done.Trigger()
}

func (s *scheduler) _schedule() {
	if r := s.ready.Next(); r != nil {
		r._addToQueue(s.running, false)
		r._unblock()
	} else {
		s.active_processors--
	}
}

func (s *scheduler) block(b State, q Queue, r ReleaseFunction) {
	s.lock.Lock()

	if q == nil {
		q = s.blocked
	}
	b._addToQueue(q, true)
	if r != nil {
		r()
	}
	s.bcnt++
	s._schedule()
	s.lock.Unlock()

	b._block()
}

func (s *scheduler) unblock(b State) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.bcnt--
	if s.active_processors < s.num_processors {
		s.active_processors++
		b._addToQueue(s.running, false)
		b._unblock()
	} else {
		b._addToQueue(s.ready, false)
	}
}

func (s *scheduler) preempt(b State) {
	s.lock.Lock()

	if r := s.ready.Next(); r != nil {
		b._addToQueue(s.ready, false)
		r._addToQueue(s.running, false)
		r._unblock()
		s.lock.Unlock()
		b._block()
	} else {
		s.lock.Unlock()
	}
}
