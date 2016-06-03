package pubsub

import (
	"net"

	"github.com/garyburd/redigo/redis"
)

// readPump tries to read out data from the connection, sending it down
// the data channel, until it's closed. It's a conversion from
// synchronous to channel-based code.
type readPump struct {
	cnx redis.Conn

	data   chan interface{}
	errs   chan error
	closer chan struct{}
}

// newReadPump creates a new pump that operates on the single Redis connection.
func newReadPump(cnx redis.Conn) *readPump {
	return &readPump{
		cnx:    cnx,
		data:   make(chan interface{}),
		errs:   make(chan error),
		closer: make(chan struct{}),
	}
}

// Work starts reading from the connection and blocks until it is closed.
func (r *readPump) Work() {
	cnx := redis.PubSubConn{Conn: r.cnx}
	defer close(r.closer)

	for {
		msg := cnx.Receive()

		if err, isErr := msg.(error); isErr && shouldNotifyUser(err) {
			select {
			case r.errs <- err:
			case <-r.closer:
				return
			}
		} else if !isErr {
			select {
			case r.data <- msg:
			case <-r.closer:
				return
			}
		}
	}
}

// Errs returns a channel of errors from the connection
func (r *readPump) Errs() <-chan error { return r.errs }

// Data returns a channel of pubsub data from the connection
func (r *readPump) Data() <-chan interface{} { return r.data }

// Close tears down the connection
func (r *readPump) Close() { r.closer <- struct{}{} }

type command struct {
	command string
	channel string
	written chan<- struct{}
}

// writePump tries to write data sent over the channel to a connection.
type writePump struct {
	cnx redis.Conn

	errs   chan error
	data   chan command
	closer chan struct{}
}

// newWritePump creates a new pump that operates on the single Redis connection.
func newWritePump(cnx redis.Conn) *writePump {
	return &writePump{
		cnx:    cnx,
		data:   make(chan command),
		errs:   make(chan error),
		closer: make(chan struct{}),
	}
}

// Work starts writing to the connection and blocks until it is closed.
func (r *writePump) Work() {
	defer close(r.closer)

	for {
		select {
		case data := <-r.data:
			r.cnx.Send(data.command, data.channel)
			if err := r.cnx.Flush(); err != nil {
				select {
				case r.errs <- err:
				case <-r.closer:
					return
				}
			}
		case <-r.closer:
			return
		}
	}
}

// Errs returns a channel of errors from the connection
func (r *writePump) Errs() <-chan error { return r.errs }

// Data returns a channel of pubsub data to be written to the connection
func (r *writePump) Data() chan<- command { return r.data }

// Close tears down the connection
func (r *writePump) Close() { r.closer <- struct{}{} }

// shouldNotifyUser return true if the user should be notified of the
// given error; if it's not a temporary network error or a timeout.
func shouldNotifyUser(err error) bool {
	if nerr, ok := err.(net.Error); ok && (nerr.Timeout() || nerr.Temporary()) {
		return false
	}

	return true
}
