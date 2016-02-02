package queue

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

// The number of errors we can get while concatting in a row before giving
// up and just returning.
const concatRetries int = 3

// ByteQueue represents either a FILO or FIFO queue contained in a particular
// Redis keyspace. It allows callers to push `[]byte` payloads, and receive them
// back over the `In() <-chan []byte`. It is typically used in a distributed
// setting, where the pusher may not always get the item back.
type ByteQueue struct {
	in     chan []byte
	closer chan struct{}
	errs   chan error
	wg     sync.WaitGroup

	name string
	pool *redis.Pool

	pmu       sync.RWMutex
	processor Processor
}

// NewByteQueue allocates and returns a pointer to a new instance of a
// ByteQueue. It initializes itself using the given *redis.Pool, and the name,
// which refers to the keyspace wherein these values will be stored.
//
// Internal channels are also initialized here.
func NewByteQueue(pool *redis.Pool, name string) *ByteQueue {
	return &ByteQueue{
		in:     make(chan []byte),
		closer: make(chan struct{}, 2),
		errs:   make(chan error),

		name: name,
		pool: pool,
	}
}

// BeginRecv spawns a goroutine that will populate the `In() <-chan []byte`. If
// BeginRecv is not called, items still may be `Push`-ed, but they will not show
// up in the In() channel, and attempting to <-In() will cause a panic.
//
// The spawned goroutine is automatically halted when `Close()` is called.
func (b *ByteQueue) BeginRecv() {
	go b.untilClosed(b.pull)
}

// Close attempts to close the polling goroutine, blocking until it is closed.
func (b *ByteQueue) Close() {
	b.closer <- struct{}{}
	b.wg.Wait()
}

// In returns a read-only chan of byte slices which are populated with items in
// the queue. If multiple instances of a ByteQueue are running in the same
// keyspace, only one will receive a particular item.
func (b *ByteQueue) In() <-chan []byte { return b.in }

// Errs returns a read-only channel of errors populated with any errors
// encountered during pushing or pulling. Errs() is a non-buffered channel.
func (b *ByteQueue) Errs() <-chan error { return b.errs }

// Push pushes the given payload (a byte slice) into the specified keyspace by
// delegating into the `Processor`'s `func Push`. It obtains a connection to
// Redis using the pool, which is passed into the Processor, and recycles that
// connection after the function has returned.
//
// If an error occurs during Pushing, it will be returned, and it can be assumed
// that the payload is not in Redis.
func (b *ByteQueue) Push(payload []byte) error {
	cnx := b.pool.Get()
	defer cnx.Close()

	err := b.Processor().Push(cnx, b.name, payload)
	if err != nil {
		return err
	}

	return nil
}

// Takes all elements from the source queue and adds them to this one. This
// can be a long-running operation. If a persistent error is returned while
// moving things, then it will be returned and the concat will stop, though
// the concat operation can be safely resumed at any time.
func (b *ByteQueue) Concat(src string) error {
	cnx := b.pool.Get()
	defer cnx.Close()

	var err error
	errCount := 0
	for {
		err = b.Processor().Concat(cnx, src, b.name)
		if err == nil {
			errCount = 0
			continue
		}

		// ErrNil is returned when there are no more items to concat
		if err == redis.ErrNil {
			return nil
		}

		// Command error are bad; something is wrong in db and we should
		// return the problem to the caller.
		if _, cmdErr := err.(redis.Error); cmdErr {
			return err
		}

		// Otherwise this is probably some temporary network error. Close
		// the old connection and try getting a new one.
		errCount++
		if errCount >= concatRetries {
			return err
		}

		cnx.Close()
		cnx = b.pool.Get()
	}
}

// Processor returns the processor that is being used to push and pull. If no
// processor is specified, a first-in-first-out will be returned by default.
func (b *ByteQueue) Processor() Processor {
	b.pmu.RLock()
	defer b.pmu.RUnlock()

	if b.processor == nil {
		return FIFO
	}

	return b.processor
}

// SetProcessor sets the current processor to the specified processor by
// aquiring a write lock into the mutex guarding that field. The processor will
// be switched over during the next iteration of a Pull-cycle, or a call to
// Push.
func (b *ByteQueue) SetProcessor(p Processor) {
	b.pmu.Lock()
	defer b.pmu.Unlock()

	b.processor = p
}

// untilClosed runs a function until an item can be read off of the `b.closer`
// channel. At the start, it increments the WaitGroup counter by one, and then
// decrements it after the `untilClosed` function returns.
func (b *ByteQueue) untilClosed(fn func()) {
	b.wg.Add(1)
	defer b.wg.Done()

	for {
		select {
		case <-b.closer:
			return
		default:
			fn()
		}
	}
}

// pull attempts to delegate into the `Processor`'s `func Pull` and handles the
// response by either passing a non-redis.ErrNil error into the `errs` channel,
// or by appending the payload into the `in` channel if no error was returned.
func (b *ByteQueue) pull() {
	cnx := b.pool.Get()
	defer cnx.Close()

	payload, err := b.Processor().Pull(cnx, b.name)
	if err == redis.ErrNil {
		return
	}

	if err != nil {
		b.errs <- err
		return
	}

	b.in <- payload
}
