package heartbeat

// Heart is a interface representing a process which runs a particular task at a
// given interval within its own goroutine.
type Heart interface {
	// Close stops the update-task from running, and frees up any owned
	// resources.
	Close()

	// Errs returns a read-only `<-chan error` which contains all errors
	// encountered while runnning the update task.
	Errs() <-chan error
}
