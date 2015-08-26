// The pubsub package provides a useful stable Redis pubsub connection.
// After opening a connection, it allows you to subscribe to and receieve
// events even in the case of network failures - you don't have to deal
// with that in your code!
//
// Basic usage, prints any messages it gets in the "foobar" channel:
//
//  client := NewPubsub("127.0.0.1:6379")
//  defer client.TearDown()
//  go client.Connect()
//
//  listener := client.Listener(Channel, "foobar")
//  for {
//      fmt.Println(<-listener.Channel)
//  }
//
// Events are emitted down the client's "Event" channel. If you wanted to
// wait until the client was open (not necessary, but may be useful):
//
//  client := NewPubsub("127.0.0.1:6379")
//  go client.Connect()
//  client.WaitFor(ConnectedEvent)
//
// You can also subscribe to patterns and unsubscribe, of course:
//
//  listener := client.Listener(Pattern, "foo:*:bar")
//  doStuff()
//  listener.Unsubscribe()

package pubsub

import (
	"net"
	"sync"
	"time"

	"github.com/WatchBeam/fsm"
	"github.com/garyburd/redigo/redis"
)

// Used to denote the parameters of the redis connection.
type ConnectionParam struct {
	// Host:port
	Address string
	// Optional password. Defaults to no authentication.
	Password string
	// Policy to use for reconnections (defaults to
	// LogReconnectPolicy with a base of 10 and factor of 1 ms)
	Policy ReconnectPolicy
	// Dial timeout for redis (defaults to no timeout)
	Timeout time.Duration
}

// Used to denote the type of listener - channel or pattern.
type ListenerType uint8

const (
	Channel ListenerType = iota
	Pattern
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
	disruptAction
)

// The Client is responsible for maintaining a subscribed redis client,
// reconnecting and resubscribing if it drops.
type Client struct {
	eventEmitter
	// The current state that the client is in.
	state     *fsm.Machine
	stateLock *sync.Mutex
	// Connection params we're subbed to.
	conn ConnectionParam
	// The subscription client we're currently using.
	pubsub *redis.PubSubConn
	// A list of events that we're subscribed to. If the connection closes,
	// we'll reestablish it and resubscribe to events.
	subscribed map[string][]*Listener
	// Reconnection policy.
	policy ReconnectPolicy
	// Channel of sub/unsub tasks
	tasks chan task
}

// Creates and returns a new pubsub client client and subscribes to it.
func New(conn ConnectionParam) *Client {
	client := &Client{
		eventEmitter: newEventEmitter(),
		state:        blueprint.Machine(),
		stateLock:    new(sync.Mutex),
		conn:         conn,
		subscribed:   map[string][]*Listener{},
		tasks:        make(chan task, 128),
	}

	if conn.Policy != nil {
		client.policy = conn.Policy
	} else {
		client.policy = &LogReconnectPolicy{Base: 10, Factor: time.Millisecond}
	}

	return client
}

// Convenience function to create a new listener for an event.
func (c *Client) Listener(kind ListenerType, event string) *Listener {
	listener := &Listener{
		Type:      kind,
		Event:     event,
		Messages:  make(chan redis.Message),
		PMessages: make(chan redis.PMessage),
		Client:    c,
	}
	c.Subscribe(listener)

	return listener
}

// Gets the current client state.
func (c *Client) GetState() uint8 {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	return c.state.State()
}

// Sets the client state in a thread-safe manner.
func (c *Client) setState(s uint8) error {
	c.stateLock.Lock()
	err := c.state.Goto(s)
	c.stateLock.Unlock()

	if err != nil {
		return err
	}

	switch s {
	case ConnectedState:
		c.emit(ConnectedEvent, nil)
	case DisconnectedState:
		c.emit(DisconnectedEvent, nil)
	case ClosingState:
		c.emit(ClosingEvent, nil)
	case ClosedState:
		c.emit(ClosedEvent, nil)
	}

	return nil
}

// Tries to reconnect to pubsub, looping until we're able to do so
// successfully. This must be called to activate the client.
func (c *Client) Connect() {
	for c.GetState() == DisconnectedState {
		go c.resubscribe()
		c.doConnection()
		time.Sleep(c.policy.Next())
	}

	c.setState(ClosedState)
}

func (c *Client) doConnection() {
	var cnx redis.Conn
	var err error
	if c.conn.Timeout > 0 {
		cnx, err = redis.DialTimeout("tcp", c.conn.Address,
			c.conn.Timeout, c.conn.Timeout, c.conn.Timeout)
	} else {
		cnx, err = redis.Dial("tcp", c.conn.Address)
	}

	if err != nil {
		c.emit(ErrorEvent, err)
		return
	}

	if c.conn.Password != "" {
		if _, err := cnx.Do("AUTH", c.conn.Password); err != nil {
			c.emit(ErrorEvent, err)
			c.setState(ClosingState)
		}
	}

	c.pubsub = &redis.PubSubConn{Conn: cnx}
	c.policy.Reset()
	c.setState(ConnectedState)

	end := make(chan bool)
	go func() {
		for {
			select {
			case <-end:
				return
			case t := <-c.tasks:
				c.workOnTask(t)
			}
		}
	}()

READ:
	for c.GetState() == ConnectedState {
		switch reply := c.pubsub.Receive().(type) {
		case redis.Message:
			go c.dispatchMessage(reply)
		case redis.PMessage:
			go c.dispatchPMessage(reply)
		case redis.Subscription:
			switch reply.Kind {
			case "subscribe", "psubscribe":
				c.emit(SubscribeEvent, reply)
			case "unsubscribe", "punsubscribe":
				c.emit(UnsubscribeEvent, reply)
			}
		case error:
			if err, ok := reply.(net.Error); ok && err.Timeout() {
				// don't emit error for time outs
			} else if c.GetState() != ConnectedState {
				// if we already closed, don't really care
			} else {
				c.emit(ErrorEvent, reply)
			}
			break READ
		}
	}

	end <- true
	c.pubsub.Close()
	c.setState(DisconnectedState)
}

// Takes a task, modifies redis and the internal subscribed registery.
// This is done here (called in boot()) since Go's maps are not thread safe.
func (c *Client) workOnTask(t task) {
	switch t.Action {
	case subscribeAction:
		event := t.Listener.Event
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
	case unsubscribeAction:
		event := t.Listener.Event
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
	case disruptAction:
		c.pubsub.Conn.Close()
	default:
		panic("unknown task")
	}
}

// Takes in a Redis message and sends it out to any "listening" channels.
func (c *Client) dispatchMessage(message redis.Message) {
	if listeners, exists := c.subscribed[message.Channel]; exists {
		for _, l := range listeners {
			l.Messages <- message
		}
	}
}

// Takes in a Redis pmessage and sends it out to any "listening" channels.
func (c *Client) dispatchPMessage(message redis.PMessage) {
	if listeners, exists := c.subscribed[message.Pattern]; exists {
		for _, l := range listeners {
			l.PMessages <- message
		}
	}
}

// Resubscribes to all Redis events. Good to do after a disconnection.
func (c *Client) resubscribe() {
	// Swap so that if we reconnect before all tasks are done,
	// we don't duplicate things.
	subs := c.subscribed
	c.subscribed = map[string][]*Listener{}

	for _, events := range subs {
		// We only need to subscribe on one event, so that the Redis
		// connection gets registered. We don't care about the others.
		c.tasks <- task{Listener: events[0], Action: subscribeAction, Force: true}
	}
}

// Tears down the client - closes the connection and stops
// listening for connections.
func (c *Client) TearDown() {
	if c.GetState() != ConnectedState {
		return
	}
	c.setState(ClosingState)
	c.tasks <- task{Action: disruptAction}

	c.WaitFor(ClosedEvent)
}

// Subscribes to a Redis event. Strings are sent back down the listener
// channel when they come in, and
func (c *Client) Subscribe(listener *Listener) {
	listener.Active = true
	c.tasks <- task{Listener: listener, Action: subscribeAction}
}

// Removes the listener from the list of subscribers. If it's the last one
// listening to that Redis event, we unsubscribe entirely.
func (c *Client) Unsubscribe(listener *Listener) {
	listener.Active = false
	c.tasks <- task{Listener: listener, Action: unsubscribeAction}
}
