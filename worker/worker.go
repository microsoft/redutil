package worker

type Worker interface {
	// Start begins the process of pulling []byte from the Queue, returning
	// them as `*Task`s on a channel of Tasks (`<-chan *Task`).
	//
	// If any errors are encountered along the way, they are sent across the
	// (unbuffered) `<-chan error`.
	Start() (<-chan *Task, <-chan error)

	// Close closes the pulling goroutine and waits for all tasks to finish
	// before returning.
	Close()

	// Halt closes the pulling goroutine, but does not wait for all tasks to
	// finish before returning.
	Halt()
}
