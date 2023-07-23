package processing

type TaskFunction[R any] func(Operation) (R, error)

type AnyTask interface {
	Start()
	RegisterAction(a TriggerAction)
	DependsOn(deps ...Dependency) error
	IsSkipped() bool
	Status() error
}

// A Task is an Execution for a TaskFunction whose execution is dependent
// on conditions provided by actions implementing the TriggerAction
// interface. Such an action might be given by a Trigger or other Task.
// Additionally, a task provides a result provided by the TaskFunction,
// which consists of an obejct of the given type parameter and an error code.
// If dependencies are again tasks, the tasks must have been succeeded without
// error to finally start the current task. If a dependent task fails,
// the current task is skipped, which can be checked with the method
// AnyTask.IsSkipped().
// The actual status (error code) can be queried by the method AnyTask.Status().
type Task[R any] interface {
	AnyTask
	Wait(Operation) (R, error)
}

type task[R any] struct {
	trigger   Trigger
	execution Execution
	deps      []AnyTask
	skipped   bool
	result    R
	err       error
}

func NewTask[R any](f TaskFunction[R], s Scheduler, names ...string) Task[R] {
	t := &task[R]{
		trigger: NewTrigger(),
	}
	t.execution = newExecution(func(op Operation) { t.run(op, f) }, s, t, "task", names...)
	t.trigger.RegisterAction(t.start)
	return t
}

func (t *task[R]) Start() {
	t.trigger.Arm()
	t.trigger.Trigger()
}

func (t *task[R]) IsSkipped() bool {
	t.execution.lock.Lock()
	defer t.execution.lock.Unlock()
	return t.skipped
}

func (t *task[R]) Status() error {
	t.execution.lock.Lock()
	defer t.execution.lock.Unlock()
	return t.err
}

func (t *task[R]) Wait(op Operation) (R, error) {
	t.execution.Wait(op)

	t.execution.lock.Lock()
	defer t.execution.lock.Unlock()
	return t.result, t.err
}

func (t *task[R]) start(Trigger) {
	t.execution.lock.Lock()
	for _, d := range t.deps {
		t.err = d.Status()
		if t.err != nil {
			break
		}
	}
	t.execution.lock.Unlock()
	if t.err == nil {
		t.execution.Start()
	} else {
		t.execution.state.skip()
		t.skipped = true
	}
}

func (t *task[R]) run(op Operation, f TaskFunction[R]) {
	r, err := f(op)

	t.execution.lock.Lock()
	defer t.execution.lock.Unlock()
	t.err = err
	t.result = r
}

func (t *task[R]) RegisterAction(a TriggerAction) {
	t.execution.RegisterAction(a)
}

func (t *task[R]) DependsOn(deps ...Dependency) error {
	t.execution.lock.Lock()
	defer t.execution.lock.Unlock()

	for _, d := range deps {
		err := t.trigger.DependOn(d)
		if err != nil {
			return err
		}
		if a, ok := d.(AnyTask); ok {
			t.deps = append(t.deps, a)
		}
	}
	return nil
}
