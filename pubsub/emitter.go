// Package pubsub implements helpers to connect to and read events from
// Redis in a reliable way.
//
// Subscriptions are handled on a Redis connection; multiple listeners can
// be attached to single events, and subscription and unsubscription on
// the connection are handled automatically. If the connection goes down,
// a new connection will be established and events will be automatically
// re-subscribed to. All methods are thread-safe.
//
// Sometimes you wish to subscribe to a simple, single event in Redis, but
// more often than not you want to subscribe to an event on a resource.
// Pubsub gives gives you an easy way to handle these kinds of subscriptions,
// without the need for `fmt` on your part or any kind or reflection-based
// formatting on ours. You create Event structures with typed fields
// and can later inspect those fields when you receive an event from
// Redis, in a reasonably fluid and very type-safe manner.
//
// Simple Event
//
//   pub := pubsub.New(cnx)
//   pub.Subscribe(pubsub.NewEvent("foo"), function (e pubsub.Event, b []byte) {
//   	// You're passed the subscribed event and any byte payload
//   	fmt.Printf("foo happened with payload %#v\n", b)
//   })
//
// Patterns and Inspections
//
//   pub := pubsub.New(cnx)
//   event := pubsub.NewPattern().
//   	String("foo:").
//   	Start().As("id").
//   	String(":bar")
//
//   pub.Subscribe(event, function (e pubsub.Event, b []byte) {
//   	// You can alias fields and look them up by their names. Pattern
//   	// events will have their "stars" filled in.
//   	id, _ := e.Find("id").Int()
//   	fmt.Printf("just got foo:%d:bar!\n", id)
//   })
package pubsub

// The Listener contains a function which, when passed to an Emitter, is
// invoked when an event occurs. It's invoked with the corresponding
// subscribed Event and the event's payload.
//
// Note that if a listener is subscribed to multiple overlapping events such
// as `foo:bar` and `foo:*`, the listener will be called multiple times.
type Listener interface {
	// Handle is invoked when the event occurs, with
	// the byte payload it contains.
	Handle(e Event, b []byte)
}

// Emitter is the primary interface to interact with pubsub.
type Emitter interface {
	// Subscribe registers that the provided lister wants to be notified
	// of the given Event. If the Listener is already subscribed to the
	// event, it will be added again and the listener will be invoked
	// multiple times when that event occurs.
	Subscribe(e EventBuilder, l Listener)

	// Unsubscribe unregisters a listener from an event. If the listener
	// is not subscribed to the event, this will be a noop. Note that this
	// only unsubscribes *one* listener from the event; if it's subscribed
	// multiple times, it will need to unsubscribe multiple times.
	Unsubscribe(e EventBuilder, l Listener)

	// Errs returns a channel of errors that occur asynchronously on
	// the Redis connection.
	Errs() <-chan error

	// Close frees resources associated with the Emitter.
	Close()
}
