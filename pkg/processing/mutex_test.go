package processing_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/processing/pkg/processing"
)

func locking(name string, prog *Stepper, lock1, lock2 processing.Mutex) processing.OperationFunction {
	return func(execution processing.Operation) {
		fmt.Printf("%s: start\n", name)
		for {
			step, ok := <-prog.stepper
			if !ok {
				fmt.Printf("%s: end\n", name)
				return
			}
			fmt.Printf("%s: got %s\n", name, step)
			prog.result.Add(step, name, "start")
			switch step {
			case LOCK:
				lock1.Lock(execution)
				prog.result.Add(step, name)
			case UNLOCK:
				lock1.Unlock()
				prog.result.Add(step, name)
			case LOCK2:
				lock2.Lock(execution)
				prog.result.Add(step, name)
			case UNLOCK2:
				lock2.Unlock()
				prog.result.Add(step, name)
			}
		}
	}
}

var _ = Describe("locking", func() {
	var sched processing.Scheduler
	var results *LockResults
	var lock1 processing.Mutex
	var lock2 processing.Mutex

	BeforeEach(func() {
		sched = processing.New(2)
		results = &LockResults{}
		lock1 = processing.NewMutex()
		lock2 = processing.NewMutex()
	})

	It("handles sequence", func() {
		fmt.Printf("start sequence\n")
		s1 := NewStepper(results)
		s2 := NewStepper(results)
		e1 := processing.NewExecution(locking("test1", s1, lock1, lock2), sched).Start()
		e2 := processing.NewExecution(locking("test2", s2, lock1, lock2), sched).Start()

		s1.Step(LOCK)
		time.Sleep(time.Second)
		s2.Step(LOCK)
		time.Sleep(time.Second)
		s2.Step(LOCK2)
		time.Sleep(time.Second)
		s1.Step(UNLOCK)
		time.Sleep(time.Second)
		s1.Step(LOCK2)
		time.Sleep(time.Second)
		s2.Step(UNLOCK)
		time.Sleep(time.Second)
		s2.Step(UNLOCK2)
		time.Sleep(time.Second)
		s1.Step(UNLOCK2)
		time.Sleep(time.Second)
		s1.Finish()
		s2.Finish()

		cnt := 0
		for {
			time.Sleep(time.Second)
			if e1.IsDone() && e2.IsDone() {
				break
			}
			cnt++
			if cnt > 10 {
				Fail("not done")
			}
		}
		Expect(results.list).To(Equal([]string{
			LOCK.S("test1"),
			LOCK.R("test1"),
			LOCK.S("test2"),
			UNLOCK.S("test1"),
			UNLOCK.R("test1"),
			LOCK.R("test2"),
			LOCK2.S("test2"),
			LOCK2.R("test2"),
			LOCK2.S("test1"),
			UNLOCK.S("test2"),
			UNLOCK.R("test2"),
			UNLOCK2.S("test2"),
			UNLOCK2.R("test2"),
			LOCK2.R("test1"),
			UNLOCK2.S("test1"),
			UNLOCK2.R("test1"),
		}))
		fmt.Printf("sequence done\n")
	})
})
