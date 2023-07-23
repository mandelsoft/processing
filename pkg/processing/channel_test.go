package processing_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/processing/pkg/processing"
)

func channel(name string, prog *Stepper, c processing.Channel[string]) processing.OperationFunction {
	return func(execution processing.Operation) {
		fmt.Printf("%s: start\n", name)
		cnt := 0
		for {
			step, ok := <-prog.stepper
			if !ok {
				fmt.Printf("%s: end\n", name)
				return
			}
			prog.result.Add(step, name, "start")
			switch step {
			case SEND:
				cnt++
				c.Send(execution, fmt.Sprintf("msg-%d", cnt))
				prog.result.Add(step, name)
			case RECEIVE:
				m, err := c.Receive(execution)
				if err != nil {
					m = err.Error()
				} else {
					prog.result.Add(step, name, m)
				}
			}
		}
	}
}

var _ = Describe("channel", func() {
	var sched processing.Scheduler
	var results *LockResults
	var ch processing.Channel[string]

	BeforeEach(func() {
		sched = processing.New(2)
		results = &LockResults{}
		ch = processing.NewChannel[string](2)
	})

	It("handles sequence", func() {
		fmt.Printf("start channel\n")

		s1 := NewStepper(results)
		s2 := NewStepper(results)

		e1 := processing.NewExecution(channel("sender", s1, ch), sched).Start()
		e2 := processing.NewExecution(channel("receiver", s2, ch), sched).Start()

		sync := processing.NewDependencyTrigger(nil, e1, e2)

		s1.Step(SEND)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)
		time.Sleep(time.Second)
		s1.Step(SEND)
		time.Sleep(time.Second)
		s1.Step(SEND)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)

		s1.Finish()
		s2.Finish()

		sync.Wait(nil)
		Expect(results.list).To(Equal([]string{
			SEND.S("sender"),
			SEND.R("sender"),
			RECEIVE.S("receiver"),
			RECEIVE.R("receiver", "msg-1"),
			RECEIVE.S("receiver"),
			SEND.S("sender"),
			RECEIVE.R("receiver", "msg-2"),
			SEND.R("sender"),
			SEND.S("sender"),
			SEND.R("sender"),
			RECEIVE.S("receiver"),
			RECEIVE.R("receiver", "msg-3"),
		}))
		fmt.Printf("channel done\n")
	})

	It("exceeds capacity", func() {
		fmt.Printf("start channel\n")

		s1 := NewStepper(results)
		s2 := NewStepper(results)

		e1 := processing.NewExecution(channel("sender", s1, ch), sched).Start()
		e2 := processing.NewExecution(channel("receiver", s2, ch), sched).Start()

		sync := processing.NewDependencyTrigger(nil, e1, e2)

		s1.Step(SEND)
		time.Sleep(time.Second)
		s1.Step(SEND)
		time.Sleep(time.Second)
		s1.Step(SEND)
		time.Sleep(time.Second)
		s1.Step(SEND)
		time.Sleep(time.Second)
		s1.Step(SEND)
		time.Sleep(time.Second)

		s2.Step(RECEIVE)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)
		time.Sleep(time.Second)
		s2.Step(RECEIVE)
		time.Sleep(time.Second)

		s1.Finish()
		s2.Finish()

		sync.Wait(nil)
		Expect(results.list).To(Equal([]string{
			SEND.S("sender"),
			SEND.R("sender"),
			SEND.S("sender"),
			SEND.R("sender"),
			SEND.S("sender"),

			RECEIVE.S("receiver"),
			SEND.R("sender"),
			SEND.S("sender"),
			RECEIVE.R("receiver", "msg-1"),
			RECEIVE.S("receiver"),
			SEND.R("sender"),
			SEND.S("sender"),
			RECEIVE.R("receiver", "msg-2"),
			RECEIVE.S("receiver"),
			SEND.R("sender"),
			RECEIVE.R("receiver", "msg-3"),
			RECEIVE.S("receiver"),
			RECEIVE.R("receiver", "msg-4"),
			RECEIVE.S("receiver"),
			RECEIVE.R("receiver", "msg-5"),
		}))
		fmt.Printf("channel done\n")
	})
})
