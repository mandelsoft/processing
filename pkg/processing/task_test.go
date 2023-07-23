package processing_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mandelsoft/processing/pkg/processing"
)

func task(name string, prog *Stepper) processing.TaskFunction[string] {
	return func(execution processing.Operation) (string, error) {
		prog.result.Add("start", name)
		return name, nil
	}
}

var _ = Describe("channel", func() {
	var sched processing.Scheduler
	var results *LockResults

	BeforeEach(func() {
		sched = processing.New(2)
		results = &LockResults{}
	})

	FIt("handles graph", func() {
		fmt.Printf("start tasks\n")

		s1 := NewStepper(results)
		s2 := NewStepper(results)
		s3 := NewStepper(results)
		s4 := NewStepper(results)

		e1 := processing.NewTask(task("t1", s1), sched)
		e2 := processing.NewTask(task("t2", s2), sched)
		e3 := processing.NewTask(task("t3", s3), sched)
		e4 := processing.NewTask(task("t4", s4), sched)

		e4.DependsOn(e2, e3)
		e3.DependsOn(e1)
		e2.DependsOn(e1, e3)

		e4.Start()
		e3.Start()
		e2.Start()
		e1.Start()
		sync := processing.NewDependencyTrigger(nil, e1, e2, e3, e4)

		s1.Finish()
		s2.Finish()
		s3.Finish()
		s4.Finish()

		sync.Wait(nil)
		Expect(results.list).To(Equal([]string{
			START.R("t1"),
			START.R("t3"),
			START.R("t2"),
			START.R("t4"),
		}))

		Expect(e1.Wait(nil)).To(Equal("t1"))
		Expect(e2.Wait(nil)).To(Equal("t2"))
		Expect(e3.Wait(nil)).To(Equal("t3"))
		Expect(e4.Wait(nil)).To(Equal("t4"))
		fmt.Printf("tasks done\n")
	})
})
