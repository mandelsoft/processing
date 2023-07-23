package processing

import (
	"fmt"
	"sync/atomic"
)

var ErrClosed = fmt.Errorf("already closed")

// Channel supports communication among executions
// by sending and receiving messages.
// It has a defined capacity to buffer sent messages.
// Once this capacity is exceeded any further Channel.Send
// operation is blocked until a message is received by the
// a Channel.Receive operation.
type Channel[T any] interface {
	Send(Operation, T) error
	Receive(Operation) (T, error)
	Close() error
}

type channel[T any] struct {
	monitor  Monitor
	send     Condition
	receive  Condition
	capacity int
	size     int
	first    int
	buffer   []T

	closed atomic.Bool
}

func NewChannel[T any](capacity int, names ...string) Channel[T] {
	return &channel[T]{
		monitor:  newMonitor("channel", names...),
		send:     NewCondition("send"),
		receive:  NewCondition("receive"),
		capacity: capacity,
		buffer:   make([]T, capacity),
	}
}

func (c *channel[T]) Send(op Operation, t T) error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.monitor.Lock(op)
	defer c.monitor.Unlock()

	for c.size >= c.capacity {
		c.monitor.Wait(c.send)
	}
	c.buffer[(c.first+c.size)%c.capacity] = t
	c.size++

	c.monitor.Notify(c.receive)
	return nil
}

func (c *channel[T]) Receive(op Operation) (T, error) {
	c.monitor.Lock(op)
	defer c.monitor.Unlock()

	for c.size == 0 {
		if c.closed.Load() {
			var zero T
			return zero, ErrClosed
		}
		c.monitor.Wait(c.receive)
	}
	t := c.buffer[c.first]
	c.size--
	c.first = (c.first + 1) % c.capacity
	c.monitor.Notify(c.send)
	return t, nil
}

func (c *channel[T]) Close() error {
	if c.closed.Swap(true) {
		return ErrClosed
	}
	return nil
}
