package processing_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/processing/pkg/processing"
)

func monitor1(name string, prog *Stepper, mon processing.Monitor, cond processing.Condition) processing.OperationFunction {
	return func(execution processing.Operation) {
		fmt.Printf("%s: start\n", name)
		for {
			step, ok := <-prog.stepper
			if !ok {
				fmt.Printf("%s: end\n", name)
				return
			}
			prog.result.Add(step, name, "start")
			switch step {
			case LOCK:
				mon.Lock(execution)
				prog.result.Add(step, name)
			case UNLOCK:
				mon.Unlock()
				prog.result.Add(step, name)
			case WAIT:
				mon.Wait(cond)
				prog.result.Add(step, name)
			case NOTIFY:
				mon.Notify(cond)
				prog.result.Add(step, name)
			}
		}
	}
}

var _ = Describe("monitor", func() {
	var sched processing.Scheduler
	var results *LockResults
	var mon processing.Monitor
	var cond processing.Condition

	BeforeEach(func() {
		sched = processing.New(2)
		results = &LockResults{}
		mon = processing.NewMonitor()
		cond = processing.NewCondition("cond")
	})

	It("handles sequence", func() {
		fmt.Printf("start monitor\n")

		s1 := NewStepper(results)
		s2 := NewStepper(results)

		e1 := processing.NewExecution(monitor1("test1", s1, mon, cond), sched).Start()
		e2 := processing.NewExecution(monitor1("test2", s2, mon, cond), sched).Start()

		sync := processing.NewDependencyTrigger(nil, e1, e2)

		s1.Step(LOCK)
		time.Sleep(time.Second)
		s2.Step(LOCK)
		time.Sleep(time.Second)
		s1.Step(WAIT)
		time.Sleep(time.Second)
		s2.Step(NOTIFY)
		time.Sleep(time.Second)
		s1.Step(UNLOCK)
		time.Sleep(time.Second)
		s2.Step(UNLOCK)
		time.Sleep(time.Second)

		s1.Finish()
		s2.Finish()

		sync.Wait(nil)
		Expect(results.list).To(Equal([]string{
			LOCK.S("test1"),
			LOCK.R("test1"),
			LOCK.S("test2"),
			WAIT.S("test1"),
			LOCK.R("test2"),
			NOTIFY.S("test2"),
			WAIT.R("test1"),
			UNLOCK.S("test1"),
			UNLOCK.R("test1"),
			NOTIFY.R("test2"),
			UNLOCK.S("test2"),
			UNLOCK.R("test2"),
		}))
		fmt.Printf("monitor done\n")
	})
})
