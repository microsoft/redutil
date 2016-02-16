package worker

import (
	"github.com/WatchBeam/redutil/queue"
)

type Lifecycle interface {
	// Sets the queues for the lifecycle's tasks. The lifecycle should
	// pull jobs from the `available` queue, then add them to the
	// `working` queue until they're complete. When they're complete,
	// the jobs can be deleted from that queue.
	SetQueues(available queue.Queue, working *queue.DurableQueue)

	// Marks a task as being completed. This is called by the Task.Complete
	// method; you should not use this directly.
	Complete(task *Task) (err error)

	// Abandon marks a task as having failed, pushing it back onto the
	// primary task queue and removing it from our worker queue. This is
	// called by the Task.Abandon method; you should not use this directly.
	Abandon(task *Task) (err error)

	// Marks *all* tasks in the queue as having been abandoned. Called by
	// the worker in the Halt() method.
	AbandonAll() (err error)

	// Starts pulling from the processing queue, returning a channel of
	// tasks and errors. Can be halted with StopListening()
	Listen() (<-chan *Task, <-chan error)

	// Stops an ongoing listening loop.
	StopListening()

	// Await blocks until all tasks currently being worked on by the Worker
	// are completed. If there are no tasks being worked on, this method
	// will return instantly.
	Await()
}
