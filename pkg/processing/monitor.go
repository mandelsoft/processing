package processing

type Condition *condition

type Monitor interface {
	Lock(Operation)
	Wait(Condition)
	Notify(Condition)
	Unlock()
}

type condition struct {
	waiting Queue
}

func NewCondition(names ...string) Condition {
	return &condition{
		waiting: NewQueue(ElementName("condition", names...)),
	}
}

type monitor struct {
	lock *mutex
}

func NewMonitor(names ...string) Monitor {
	return newMonitor("monitor", names...)
}

func newMonitor(typ string, names ...string) Monitor {
	return &monitor{
		lock: &mutex{
			waiting: NewQueue(ElementName(typ, names...)),
		},
	}
}

func (m *monitor) Lock(op Operation) {
	m.lock.Lock(op)
}

func (m *monitor) Wait(c Condition) {
	m.lock.lock.Lock()

	if m.lock.holder == nil {
		m.lock.lock.Unlock()
		panic("wait executed outside monitor")
	}
	holder := m.lock.holder
	m.lock.holder.Block(c.waiting, m.lock.unlock)
	m.lock.holder = holder
}

func (m *monitor) Notify(c Condition) {
	m.lock.lock.Lock()

	if m.lock.holder == nil {
		m.lock.lock.Unlock()
		panic("wait executed outside monitor")
	}
	holder := m.lock.holder

	if n := c.waiting.Next(); n != nil {
		m.lock.lock.Unlock()
		n.Unblock()    // pass monitor lock to unblocked wait
		m.Lock(holder) // reacquire lock to continue
	} else {
		m.lock.lock.Unlock()
	}
}

func (m *monitor) Unlock() {
	m.lock.Unlock()
}
