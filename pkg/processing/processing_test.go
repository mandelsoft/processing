package processing_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/mandelsoft/processing/pkg/processing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type SimpleResult struct {
	lock sync.Mutex

	result map[string]string
}

func NewSimpleResult() *SimpleResult {
	return &SimpleResult{
		result: map[string]string{},
	}
}

func (s *SimpleResult) Set(name, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.result[name] = value
}

func simple(name string, result *SimpleResult) processing.OperationFunction {
	return func(execution processing.Operation) {
		result.Set(name, "done")
	}
}

func waiting(name string, p processing.Execution, result *SimpleResult) processing.OperationFunction {
	return func(execution processing.Operation) {

		if p != nil {
			p.Wait(execution)
		}
		result.Set(name, "done")
	}
}

var _ = Describe("simple processing", func() {
	var sched processing.Scheduler
	var results *SimpleResult

	BeforeEach(func() {
		sched = processing.New(2)
		results = NewSimpleResult()
	})

	It("processes single execution", func() {
		fmt.Printf("start single execution\n")
		e1 := processing.NewExecution(simple("test1", results), sched).Start()

		cnt := 0
		for {
			time.Sleep(time.Second)
			if e1.IsDone() {
				break
			}
			cnt++
			if cnt > 10 {
				Fail("not done")
			}
		}
		Expect(results.result).To(Equal(map[string]string{"test1": "done"}))
		fmt.Printf("single execution done\n")
	})

	It("processes synched execution", func() {
		fmt.Printf("start simple execution\n")
		e1 := processing.NewExecution(simple("test1", results), sched).Start()
		e2 := processing.NewExecution(simple("test2", results), sched).Start()
		e3 := processing.NewExecution(simple("test3", results), sched).Start()

		cnt := 0
		for {
			time.Sleep(time.Second)
			if e1.IsDone() && e2.IsDone() && e3.IsDone() {
				break
			}
			cnt++
			if cnt > 10 {
				Fail("not done")
			}
		}
		Expect(results.result).To(Equal(map[string]string{"test1": "done", "test2": "done", "test3": "done"}))
		fmt.Printf("simple execution done\n")
	})

	It("processes synched execution", func() {
		fmt.Printf("start synched execution\n")
		e1 := processing.NewExecution(waiting("test1", nil, results), sched)
		e2 := processing.NewExecution(waiting("test2", e1, results), sched)
		e3 := processing.NewExecution(waiting("test3", e2, results), sched)

		e3.Start()
		e2.Start()
		e1.Start()

		cnt := 0
		for {
			time.Sleep(time.Second)
			if e1.IsDone() && e2.IsDone() && e3.IsDone() {
				break
			}
			cnt++
			if cnt > 10 {
				Fail("not done")
			}
		}
		Expect(results.result).To(Equal(map[string]string{"test1": "done", "test2": "done", "test3": "done"}))
		fmt.Printf("synched execution done\n")
	})

	It("external sync", func() {
		fmt.Printf("start external sync\n")
		e1 := processing.NewExecution(waiting("test1", nil, results), sched)
		e2 := processing.NewExecution(waiting("test2", e1, results), sched)
		e3 := processing.NewExecution(waiting("test3", e2, results), sched)

		sync := processing.NewDependencyTrigger(nil, e1, e2, e3)

		e3.Start()
		e2.Start()
		e1.Start()

		sync.Wait(nil)

		Expect(e1.IsDone()).To(BeTrue())
		Expect(e2.IsDone()).To(BeTrue())
		Expect(e3.IsDone()).To(BeTrue())

		Expect(results.result).To(Equal(map[string]string{"test1": "done", "test2": "done", "test3": "done"}))
		fmt.Printf("external sync done\n")
	})

})
