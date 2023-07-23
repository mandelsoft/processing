package processing

import (
	"sync"
)

type Mutex = *mutex

type mutex struct {
	lock sync.Mutex

	waiting Queue
	locked  bool
	holder  Operation
}

func NewMutex(names ...string) Mutex {
	return &mutex{
		waiting: NewQueue(ElementName("mutex", names...)),
	}
}

func (m *mutex) Lock(o Operation) {
	m.lock.Lock()

	for m.locked {
		o.Block(m.waiting, m.lock.Unlock)
	}
	m.holder = o
	m.locked = true
	m.lock.Unlock()
}

func (m *mutex) Unlock() {
	m.lock.Lock()
	m.unlock()
}

func (m *mutex) unlock() {
	if !m.locked {
		panic("unlocking unlocked mutex")
	}
	m.holder = nil
	m.locked = false
	if n := m.waiting.Next(); n != nil {
		go n.Unblock() // pass lock
	} else {
		m.lock.Unlock()
	}
}
