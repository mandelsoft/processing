package processing_test

import (
	"fmt"
	"strings"
	"sync"
)

type LockResults struct {
	lock sync.Mutex
	list []string
}

func (l *LockResults) Add(step Step, e ...string) {
	l.lock.Lock()
	defer l.lock.Unlock()

	msg := step.R(strings.Join(e, " "))

	fmt.Printf("%s\n", msg)
	l.list = append(l.list, msg)
}

func (l *LockResults) Len() int {
	l.lock.Lock()
	defer l.lock.Unlock()
	return len(l.list)
}

////////////////////////////////////////////////////////////////////////////////

type Step string

func (c Step) R(ctx ...string) string {
	return strings.Join(ctx, " ") + ": " + string(c)
}

func (c Step) S(ctx string) string {
	return ctx + " start: " + string(c)
}

const (
	START = Step("start")

	LOCK    = Step("lock")
	LOCK2   = Step("lock2")
	UNLOCK  = Step("unlock")
	UNLOCK2 = Step("unlock2")
	WAIT    = Step("wait")
	NOTIFY  = Step("notify")
	SEND    = Step("send")
	RECEIVE = Step("receive")
)

type Stepper struct {
	result  *LockResults
	stepper chan Step
}

func NewStepper(result *LockResults) *Stepper {
	return &Stepper{result, make(chan Step, 100)}
}

func (s *Stepper) Step(step Step) {
	s.stepper <- step
}

func (s *Stepper) Finish() {
	close(s.stepper)
}
