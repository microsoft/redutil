// The pubsub package provides a useful stable Redis pubsub connection.
// After opening a connection, it allows you to subscribe to and receieve
// events even in the case of network failures - you don't have to deal
// with that in your code!
//
// Basic usage, prints any messages it gets in the "foobar" channel:
//
// 	client := NewPubsub("127.0.0.1:6379")
// 	defer client.TearDown()
// 	go client.Connect()
//
// 	listener := client.Listener(Channel, "foobar")
// 	for {
// 		fmt.Println(<-listener.Channel)
// 	}
//
// Events are emitted down the client's "Event" channel. If you wanted to
// wait until the client was open (not necessary, but may be useful):
//
// 	client := NewPubsub("127.0.0.1:6379")
// 	go client.Connect()
// 	client.WaitFor(ConnectedEvent)
//
// You can also subscribe to patterns and unsubscribe, of course:
//
// 	listener := client.Listener(Pattern, "foo:*:bar")
// 	doStuff()
// 	listener.Unsubscribe()
//
package pubsub

import (
	"github.com/garyburd/redigo/redis"
	"math"
	"sync"
	"time"
)

// The Client is responsible for maintaining a subscribed redis client,
// reconnecting and resubscribing if it drops.
type Client struct {
	// Event channel you can pull from to know what's happening in the app.
	// By default this is a buffered channel with a size of 1.
	Events chan Event
	// The current state that the client is in.
	State State
	// Lock used to force thread safety on the state.
	stateLock *sync.Mutex
	// Lock used to force thread safety on the pubsub client.
	pubsubLock *sync.Mutex
	// The address we're connected to
	address string
	// The subscription client we're currently using.
	pubsub *redis.PubSubConn
	// How many times we've tried to reconnect.
	retries uint
	// A list of events that we're subscribed to. If the connection closes,
	// we'll reestablish it and resubscribe to events.
	subscribed map[string][]*Listener
	// A queue so that we can asynchronously send "todos" to the main
	// subscribe routine to listen and unsubscribe from events.
	actionQueue chan task
}

// Used to denote the type of listener - channel or pattern.
type ListenerType uint8

const (
	Channel ListenerType = iota
	Pattern
)

// The event is sent down the client's Events channel when something happens!
type Event struct {
	Type   EventType
	Packet interface{}
}

// Events that are sent down the "Events" channel.
type EventType uint8

const (
	ConnectedEvent EventType = iota
	DisconnectedEvent
	MessageEvent
	SubscribeEvent
	UnsubscribeEvent
)

// Tasks we send to the main pubsub thread to subscribe/unsubscribe.
type task struct {
	Action   action
	Listener *Listener
	// If true, the *action* (subscribed/unsubscribed) will be undertaken
	// even if we think we might have already done it.
	Force bool
}

// actions are "things" we can do in tasks
type action uint8

// List of actions we can use in tasks. Internal use
const (
	subscribeAction action = iota
	unsubscribeAction
)

// Client state information
type State uint8

const (
	// Not currently connected to a server.
	DisconnectedState State = iota
	// We were connected, but closed gracefully.
	ClosedState
	// Currently in the process of trying to connect to a server.
	ConnectingState
	// Connected to a server, but not yet linked as a pubsub client.
	ConnectedState
	// Fully connected and listening for events.
	BootedState
)

// Creates a new Radix client and subscribes to it.
func New(address string) *Client {
	client := &Client{
		Events:      make(chan Event),
		State:       DisconnectedState,
		address:     address,
		subscribed:  map[string][]*Listener{},
		actionQueue: make(chan task, 10),
		stateLock:   new(sync.Mutex),
		pubsubLock:  new(sync.Mutex),
	}

	return client
}

// Convenience function to create a new listener for an event.
func (c *Client) Listener(kind ListenerType, event string) *Listener {
	listener := &Listener{
		Type:      kind,
		Event:     event,
		Messages:  make(chan *redis.Message),
		PMessages: make(chan *redis.PMessage),
		Client:    c,
	}
	c.Subscribe(listener)

	return listener
}

// Tears down the client - closes the connection and stops listening for
// connections.
func (c *Client) TearDown() {
	c.setState(ClosedState)

	if c.pubsub != nil {
		c.pubsub.Close()
	}
}

// Gets the current client state.
func (c *Client) GetState() State {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	return c.State
}

// Sets the client state in a thread-safe manner.
func (c *Client) setState(s State) {
	c.stateLock.Lock()
	c.State = s
	c.stateLock.Unlock()
}

// Blocks until a certain event type is emitted on the client.
func (c *Client) WaitFor(event EventType) Event {
	for {
		if ev := <-c.Events; ev.Type == event {
			return ev
		}
	}
}

// Creates a connection for the client, based on the address. Returns
// an error if there was any issues.
func (c *Client) setupConnection() error {
	cnx, err := redis.Dial("tcp", c.address)
	if err != nil {
		return err
	}

	c.pubsubLock.Lock()
	c.pubsub = &redis.PubSubConn{Conn: cnx}
	c.pubsubLock.Unlock()

	c.retries = 0
	c.setState(ConnectedState)
	c.emit(Event{Type: ConnectedEvent})

	return nil
}

// Should be called when the client is opened. Recieves events, reconnects,
// and dispatches incoming messages.
func (c *Client) boot() {
	c.setState(BootedState)

	halt := make(chan bool, 1)
	ch := make(chan interface{})
	go func() {

		for {
			c.pubsubLock.Lock()
			e := c.pubsub.Receive()
			c.pubsubLock.Unlock()

			if !<-halt {
				ch <- e
			} else {
				return
			}
		}
	}()

	halt <- false
	for c.GetState() == BootedState {

		// Listen for both pubsub receives and new things we have to
		// subscribe to...
		select {
		case e := <-c.actionQueue:
			c.workOnTask(e)
		case e := <-ch:
			halt <- c.dispatchReply(e)
		}
	}
}

// Takes a task, modifies redis and the internal subscribed registery.
// This is done here (called in boot()) since Go's maps are not thread safe.
func (c *Client) workOnTask(t task) {
	event := t.Listener.Event

	// If the action is subscribe...
	if t.Action == subscribeAction {

		// Check to see if it already exists in the subscribed map.
		// If not, then we need to create a new listener list and
		// start listening on the pubsub client. Otherwise just
		// append it.
		if _, exists := c.subscribed[event]; !exists || t.Force {
			c.subscribed[event] = []*Listener{t.Listener}

			switch t.Listener.Type {
			case Channel:
				c.pubsub.Subscribe(event)
			case Pattern:
				c.pubsub.PSubscribe(event)
			}
		} else {
			c.subscribed[event] = append(c.subscribed[event], t.Listener)
		}
	} else {
		// Look for the listener in the subscribed list and,
		// once found, remove it.
		list := c.subscribed[event]
		for i, l := range list {
			if l == t.Listener {
				c.subscribed[event] = append(list[:i], list[i+1:]...)
				break
			}
		}

		// If the list is now empty, we can unsubscribe on Redis and
		// remove it from our registery
		if len(c.subscribed[event]) == 0 || t.Force {
			switch t.Listener.Type {
			case Channel:
				c.pubsub.Unsubscribe(event)
			case Pattern:
				c.pubsub.PUnsubscribe(event)
			}
			delete(c.subscribed, event)
		}
	}
}

// Takes a reply from Redis' recieve and dispatches the
// appropriate message. Returns whether the event loop should be halted.
func (c *Client) dispatchReply(r interface{}) bool {
	switch reply := r.(type) {
	case redis.Message:
		c.dispatchMessage(&reply)
	case redis.PMessage:
		c.dispatchPMessage(&reply)
	case redis.Subscription:
		switch reply.Kind {
		case "subscribe", "psubscribe":
			c.emit(Event{Type: SubscribeEvent, Packet: &reply})
		case "unsubscribe", "punsubscribe":
			c.emit(Event{Type: UnsubscribeEvent, Packet: &reply})
		}
	case error:
		c.handleError(reply)
		return true
	}

	return false
}

// Emits a non-blocking event on the client.
func (c *Client) emit(event Event) {
	select {
	case c.Events <- event:
	default:
	}
}

// Returns the delay time before we should try to reconnect. Increases
// in log time.
func (c *Client) getNextDelay() time.Duration {
	c.retries += 1
	return time.Duration(math.Log(float64(c.retries)) * float64(time.Millisecond))
}

// Returns whether the pubsub client is in a state that you can connect to.
func (c *Client) clientAvailable() bool {
	s := c.GetState()
	return s == ConnectedState || s == BootedState
}

// Tries to reconnect to pubsub, looping until we're
// able to do so successfully.
func (c *Client) Connect() {
	c.setState(ConnectingState)

	for !c.clientAvailable() {
		time.Sleep(c.getNextDelay())
		if c.GetState() == ClosedState {
			return
		}

		c.setupConnection()
	}

	c.resubscribe()
	c.boot()
}

// Resubscribes to all Redis events. Good to do after a disconnection.
func (c *Client) resubscribe() {
	for _, events := range c.subscribed {
		// We only need to subscribe on one event, so that the Redis
		// connection gets registered. We don't care about the others.
		c.actionQueue <- task{Listener: events[0], Action: subscribeAction, Force: true}
	}
}

// Handles error that occur in redis pubsub. Reconnects if necessary.
func (c *Client) handleError(err error) {
	c.emit(Event{Type: DisconnectedEvent, Packet: err})

	if c.GetState() != DisconnectedState {
		c.setState(DisconnectedState)
		c.pubsub.Close()
		c.Connect()
	}
}

// Takes in a Redis message and sends it out to any "listening" channels.
func (c *Client) dispatchMessage(message *redis.Message) {
	if listeners, exists := c.subscribed[message.Channel]; exists {
		for _, l := range listeners {
			l.Messages <- message
		}
	}
}

// Takes in a Redis pmessage and sends it out to any "listening" channels.
func (c *Client) dispatchPMessage(message *redis.PMessage) {
	if listeners, exists := c.subscribed[message.Pattern]; exists {
		for _, l := range listeners {
			l.PMessages <- message
		}
	}
}

// Subscribes to a Redis event. Strings are sent back down the listener
// channel when they come in, and
func (c *Client) Subscribe(listener *Listener) {
	listener.Active = true
	c.actionQueue <- task{Listener: listener, Action: subscribeAction}
}

// Removes the listener from the list of subscribers. If it's the last one
// listening to that Redis event, we unsubscribe entirely.
func (c *Client) Unsubscribe(listener *Listener) {
	listener.Active = false
	c.actionQueue <- task{Listener: listener, Action: unsubscribeAction}
}
