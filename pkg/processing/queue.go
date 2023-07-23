package processing

import (
	"sync"
)

type Queue interface {
	Name() string
	Add(b Operation)
	Remove(b Operation) bool
	Len() int
	Next() Operation
}

type queue struct {
	lock sync.Mutex
	name string
	list []Operation
}

func NewQueue(name string) Queue {
	return &queue{name: name}
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) Add(b Operation) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.list = append(q.list, b)
}

func (q *queue) Len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return len(q.list)
}

func (q *queue) Next() Operation {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.list) > 0 {
		r := q.list[0]
		q.list = q.list[1:]
		r._removedFromQueue(q)
		return r
	}
	return nil
}

func (q *queue) Remove(b Operation) bool {
	if q == nil {
		return false
	}
	q.lock.Lock()
	defer q.lock.Unlock()

	for i, e := range q.list {
		if e == b {
			q.list = append(q.list[:i], q.list[i+1:]...)
			return true
		}
	}
	return false
}
