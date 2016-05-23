// The pubsub package implements helpers to connect to and read events from
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
//   event := pubsub.NewPatternEvent("foo:",
//   	pubsub.Star().As("id"),
//   	pubsub.String(":bar"))
//
//   pub.Subscribe(event, function (e pubsub.Event, b []byte) {
//   	// You can alias fields and look them up by their names. Pattern
//   	// events will have their "stars" filled in.
//   	id, _ := e.Find("id").Int()
//   	fmt.Printf("just got foo:%d:bar!\n", id)
//   })
package pubsub

// ErrSubscribed is returned from Emitter.Subscribe when the consumer attempts
// to subscribe a single listener to an event multiple times.
type ErrSubscribed struct{ e Event }

// Error implements error.Error
func (e ErrSubscribed) Error() string {
	return "redutil/pubsub2: attempted to duplicate " +
		"subscription to `" + e.e.Name() + "`"
}

// ErrNotSubscribed is returned from Emitter.Unsubscribe when the consumer
// attempts to unsubscribe a listener who was not subscribed to an event.
type ErrNotSubscribed struct{ e Event }

// Error implements error.Error
func (e ErrNotSubscribed) Error() string {
	return "redutil/pubsub2: attempted to unsubscribe from `" +
		e.e.Name() + "` but was not subscribed!"
}

// The Listener is a function which, when passed to an Emitter, is invoked
// when an event occurs. It's invoked with the corresponding subscribed
// Event and the event's payload.
//
// Note that if a listener is subscribed to multiple overlapping events such
// as `foo:bar` and `foo:*`, the choice of the Event the Listener is
// called with is undefined.
type Listener func(e Event, b []byte)

type Emitter interface {
	// Subscribe registers that the provided lister wants to be notified
	// of the given Event. If the Listener is already subscribed to the
	// event, an ErrSubscribed error will be returned.
	Subscribe(e Event, l Listener) error

	// Unsubscribe unregisters a listener from an event. If the listener
	// is not subscribed to the event, an ErrNotSubscribed error will
	// be returned.
	Unsubscribe(e Event, l Listener) error
}
