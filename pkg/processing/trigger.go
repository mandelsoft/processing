package processing

import (
	"fmt"
	"sync"
)

var ErrArmed = fmt.Errorf("trigger already armed")

type TriggerAction func(Trigger)

type Dependency interface {
	RegisterAction(TriggerAction)
}

// Trigger is an object which can be used to synchronize
// operations. Operations can wait for a Trigger to reach
// the triggered state, meaning:
// - the trigger is armed
// - all dependencies have been fired
// - the Trigger.Trigger() method is called
// A Trigger optionally fires an action (function) if it
// reaches the triggered state. This function is executed only once.
// Optionally a Trigger may depend on dependencies given by
// an object implementing the Dependency interface providing
// a method to register a TriggerAction. Registered action function MUST only be
// executed once.
// This way a Trigger may be triggered by other Triggers.
type Trigger interface {
	Dependency

	DependOn(...Dependency) error
	Arm()
	Trigger()

	IsTriggered() bool

	Wait(operation Operation)
}

// NewTrigger creates a generic unarmed Trigger.
func NewTrigger(names ...string) Trigger {
	return &trigger{waiting: NewQueue(ElementName("trigger", names...))}
}

// NewArmedTrigger creates an already armed Trigger configure with
// a set of dependencies and a TriggerAction.
func NewArmedTrigger(a TriggerAction, deps ...Dependency) Trigger {
	t := NewTrigger()
	for _, d := range deps {
		t.DependOn(d)
	}
	t.RegisterAction(a)
	t.Arm()
	return t
}

// NewDependencyTrigger creates an armed Trigger, which triggers
// when all dependencies have been fired.
func NewDependencyTrigger(a TriggerAction, deps ...Dependency) Trigger {
	t := NewArmedTrigger(a, deps...)
	t.Trigger()
	return t
}

type trigger struct {
	lock sync.Mutex

	actions []func(Trigger)

	armed        bool
	triggered    bool
	dependencies int

	waiting Queue
}

func (t *trigger) Arm() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.armed = true
	t.trigger()
}

func (t *trigger) Trigger() {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.triggered {
		t.triggered = true
		t.trigger()
	}
}

func (t *trigger) trigger() {
	if t.isTriggered() {
		for _, a := range t.actions {
			a(t)
		}

		for {
			if n := t.waiting.Next(); n != nil {
				n.Unblock()
			} else {
				break
			}
		}
	}
}

func (t *trigger) RegisterAction(a TriggerAction) {
	if a == nil {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	t.registerAction(a)
}

func (t *trigger) registerAction(a TriggerAction) {
	if t.isTriggered() {
		a(t)
	} else {
		t.actions = append(t.actions, a)
	}
}

func (t *trigger) depTriggered(d Trigger) {
	t.lock.Lock()
	t.dependencies--
	t.trigger()
	t.lock.Unlock()
}

func (t *trigger) DependOn(deps ...Dependency) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.armed {
		return ErrArmed
	}
	for _, d := range deps {
		t.dependencies++
		d.RegisterAction(t.depTriggered)
	}
	return nil
}

func (t *trigger) IsTriggered() bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.isTriggered()
}

func (t *trigger) isTriggered() bool {
	return t.triggered && t.armed && t.dependencies == 0
}

// Wait waits for the trigger to reach the triggered state, meaning
// - it must be armed
// - it must be triggered
// - all dependencies must have fired.
// If the operation is given it is executed inside an operation
// scheduled by the scheduler
// and the operation is blocked by the scheduler until the trigger
// reached the triggered state. Hereby, the scheduler is able to
// continue with another operation ready for execution.
// If it is NOT given (nil), the actual GO routine is blocked
// by the GO runtime instead of using the scheduler of an operation.
func (t *trigger) Wait(op Operation) {
	t.lock.Lock()

	if !t.isTriggered() {
		if op == nil {
			var lock sync.Mutex
			lock.Lock()
			t.registerAction(func(Trigger) { lock.Unlock() })
			t.lock.Unlock()
			lock.Lock()
		} else {
			op.Block(t.waiting, t.lock.Unlock)
		}
	} else {
		t.lock.Unlock()
	}
}
