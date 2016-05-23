package pubsub

import (
	"strconv"
	"strings"
)

// Fields are concatenated into events which can
// be listened to over liveloading.
type Field struct {
	// Value is the underlying value of the field
	Value interface{}
	alias string
	str   string
}

// As sets the alias of the field in the event list. You may then call
// Event.Find(alias) to look up the value of the field in the event.
func (f Field) As(alias string) Field {
	f.alias = alias
	return f
}

// String creates and returns a Field containing a string.
func String(str string) Field { return Field{Value: str, str: str} }

// Int creates and returns a Field containing an integer.
func Int(x int) Field { return Field{Value: x, str: strconv.Itoa(x)} }

// Star returns a field containing the Kleene star `*` for pattern subscription.
func Star() Field { return Field{Value: "*", str: "*"} }

// An Event is passed to an Emitter to manage which
// events a Listener is subscribed to.
type Event struct {
	fields []Field

	sub   string
	unsub string
}

// Len returns the number of fields contained in the event.
func (e *Event) Len() int { return len(e.fields) }

// Get returns the value of a field at index `i` within the event. If the
// field does not exist, nil will be returned.
func (e *Event) Get(i int) interface{} {
	if len(e.fields) >= i {
		return nil
	}

	return e.fields[i].Value
}

// Find looks up a field value by its alias. This is most useful in pattern
// subscriptions where might use Find to look up a parameterized property.
func (e *Event) Find(alias string) interface{} {
	for _, field := range e.fields {
		if field.alias == alias {
			return field.Value
		}
	}

	return nil
}

// Name returns name of the event, formed by a concatenation of all the
// event fields.
func (e *Event) Name() string {
	strs := make([]string, len(e.fields))
	for i, field := range e.fields {
		strs[i] = field.str
	}

	return strings.Join(strs, "")
}

// NewEvent creates and returns a new event based off the series of fields.
// This translates to a Redis SUBSCRIBE call.
func NewEvent(str string, fields ...Field) Event {
	return Event{
		fields: append([]Field{String(str)}, fields...),
		sub:    "SUBSCRIBE",
		unsub:  "UNSUBSCRIBE",
	}
}

// NewPatternEvent creates and returns a new event pattern off the series
// of fields. This translates to a Redis PSUBSCRIBE call.
func NewPatternEvent(str string, fields ...Field) Event {
	return Event{
		fields: append([]Field{String(str)}, fields...),
		sub:    "PSUBSCRIBE",
		unsub:  "PUNSUBSCRIBE",
	}
}
