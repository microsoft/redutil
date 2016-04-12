package worker

import (
	"encoding/hex"
	"errors"
	"sync"
)

// ErrAlreadyResolved is returned from Task.Fail or Task.Succeed if the Task
// was already marked as having failed or resolved.
var ErrAlreadyResolved = errors.New("Attempted to resolve an already-resolved task.")

// A Task encapsulates a item coming off the main queue that needs to be
// "worked" on. Upon receiving a Task, callers may either Complete() or Fail()
// the task, which will be delegated into the lifecycle appropriately.
//
// Should execution be halted, the Closer() channel will get an empty "message"
// that can be read off, which means that execution has indeed been halted.
type Task struct {
	lifecycle Lifecycle
	// payload is the data that this Task is holding
	payload []byte
	// Whether the task has already been marked as succeeded or failed.
	resolved   bool
	resolvedMu sync.Mutex
}

// NewTask initializes and returns a pointer to a new Task instance. It is
// constructed with the given payload, progress, and closer channels
// respectively, all of which should be open when either Succeed(), Fail() or
// Closer() is called.
func NewTask(lifecycle Lifecycle, payload []byte) *Task {
	return &Task{lifecycle: lifecycle, payload: payload}
}

// Bytes returns the bytes that this Task is holding, and is the "data" to be
// worked on.
func (t *Task) Bytes() []byte { return t.payload }

// Succeed signals the lifecycle that work on this task has been completed,
// and removes the task from the worker queue.
func (t *Task) Succeed() error {
	return t.guardResolution(func() error {
		return t.lifecycle.Complete(t)
	})
}

// Fail signals the lifecycle that work on this task has failed, causing
// it to return the task to the main processing queue to be retried.
func (t *Task) Fail() error {
	return t.guardResolution(func() error {
		return t.lifecycle.Abandon(t)
	})
}

// IsResolved returns true if the task has already been marked as having
// succeeded or failed.
func (t *Task) IsResolved() bool {
	t.resolvedMu.Lock()
	defer t.resolvedMu.Unlock()

	return t.resolved
}

// HexDump returns a byte dump of the task, in the same format as `hexdump -C`.
// This is useful for debugging/logging purposes.
func (t *Task) HexDump() string {
	return hex.Dump(t.Bytes())
}

// String returns the strinigified contents of the task payload.
func (t *Task) String() string {
	return string(t.Bytes())
}

// guardResolution runs the inner fn only if the task is not already resolved,
// returning ErrAlreadyResolved if that's not the case. If the inner function
// returns no error, the task will subsequently be marked as resolved.
func (t *Task) guardResolution(fn func() error) error {
	t.resolvedMu.Lock()
	defer t.resolvedMu.Unlock()
	if t.resolved {
		return ErrAlreadyResolved
	}

	err := fn()
	if err == nil {
		t.resolved = true
	}

	return err
}
