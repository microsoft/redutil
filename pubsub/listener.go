package pubsub

import "github.com/garyburd/redigo/redis"

// The listener is used to keep track of events that the client is
// listening to.
type Listener struct {
	// The event slug we're listening for.
	Event string
	// and its type
	Type ListenerType
	// The channel we send events down for plain subscriptions.
	Messages chan redis.Message
	// The channel we send events down for pattern subscriptions.
	PMessages chan redis.PMessage
	// The client it's attached to.
	Client *Client
	// Whether its active. True by default, false after unsubscribed.
	Active bool
}

// Unsubscribes the listener.
func (l *Listener) Unsubscribe() {
	l.Client.Unsubscribe(l)
}

// Resubscribes the listener.
func (l *Listener) Resubscribe() {
	l.Client.Subscribe(l)
}
