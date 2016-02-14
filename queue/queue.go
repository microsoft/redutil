package queue

import "time"

type Queue interface {
	// Source returns the keyspace in Redis from which this queue is
	// populated.
	Source() string

	// Push pushes the given payload (a byte slice) into the specified
	// keyspace by delegating into the `Processor`'s `func Push`. It obtains
	// a connection to Redis using the pool, which is passed into the
	// Processor, and recycles that connection after the function has
	// returned.
	//
	// If an error occurs during Pushing, it will be returned, and it can be
	// assumed that the payload is not in Redis.
	Push(payload []byte) (err error)

	// Pull returns the next available payload, blocking until data can be
	// returned.
	Pull(timeout time.Duration) (payload []byte, err error)

	// Takes all elements from the source queue and adds them to this one. This
	// can be a long-running operation. If a persistent error is returned while
	// moving things, then concat will stop, though the concat operation can
	// be safely resumed at any time.
	//
	// Returns the number of items successfully moved and any error that
	// occurred.
	Concat(src string) (moved int, err error)

	// Processor returns the processor that is being used to push and pull.
	// If no processor is specified, a first-in-first-out will be returned
	// by default.
	Processor() Processor

	// SetProcessor sets the current processor to the specified processor by
	// aquiring a write lock into the mutex guarding that field. The
	// processor will be switched over during the next iteration of a
	// Pull-cycle, or a call to Push.
	SetProcessor(processor Processor)
}
