package pubsub

import (
	"sync"
)

// The event is sent down the client's Events channel when something happens!
type Event struct {
	Type   EventType
	Packet interface{}
}

// A function which handles an incoming event.
type EventHandler func(Event)

// Events that are sent down the "Events" channel.
type EventType uint8

const (
	ConnectedEvent EventType = iota
	DisconnectedEvent
	ClosingEvent
	ClosedEvent
	MessageEvent
	SubscribeEvent
	UnsubscribeEvent
	ErrorEvent
	AnyEvent
)

// Simple implementation of a Node-like event emitter.
type eventEmitter struct {
	lock      *sync.Mutex
	listeners map[EventType][]EventHandler
	once      map[EventType][]EventHandler
}

func newEventEmitter() eventEmitter {
	return eventEmitter{
		lock:      new(sync.Mutex),
		listeners: map[EventType][]EventHandler{},
		once:      map[EventType][]EventHandler{},
	}
}

func (e *eventEmitter) addHandlerToMap(ev EventType, h EventHandler, m map[EventType][]EventHandler) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if handlers, ok := m[ev]; ok {
		m[ev] = append(handlers, h)
	} else {
		m[ev] = []EventHandler{h}
	}
}

// Adds a handler that's executed once when an event is emitted.
func (e *eventEmitter) Once(ev EventType, h EventHandler) {
	e.addHandlerToMap(ev, h, e.once)
}

// Adds a handler that's executed when an event happens.
func (e *eventEmitter) On(ev EventType, h EventHandler) {
	e.addHandlerToMap(ev, h, e.listeners)
}

// Creates a channel that gets written to when a new event comes in.
func (e *eventEmitter) OnChannel(ev EventType) chan Event {
	ch := make(chan Event, 1)
	e.On(ev, func(e Event) {
		ch <- e
	})

	return ch
}

// Triggers an event to be sent out to listeners.
func (e *eventEmitter) emit(typ EventType, data interface{}) {
	ev := Event{Type: typ, Packet: data}

	e.lock.Lock()
	lists := [][]EventHandler{}
	if handlers, ok := e.listeners[ev.Type]; ok {
		lists = append(lists, handlers)
	}
	if handlers, ok := e.once[ev.Type]; ok {
		lists = append(lists, handlers)
		delete(e.once, ev.Type)
	}
	e.lock.Unlock()

	for _, list := range lists {
		for _, handler := range list {
			go handler(ev)
		}
	}
}

// Blocks until an event is received. Mainly for backwards-compatibility.
func (e *eventEmitter) WaitFor(ev EventType) {
	done := make(chan bool)

	go func() {
		e.Once(ev, func(e Event) {
			done <- true
		})
	}()

	<-done
}
