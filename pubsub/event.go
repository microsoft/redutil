package pubsub

import (
	"fmt"
	"strconv"
	"strings"
)

// EventType is used to distinguish between pattern and plain text events.
type EventType int

const (
	// PlainEvent is the event type for events that are simple subscribed
	// to in Redis without pattern matching.
	PlainEvent EventType = iota + 1
	// PatternEvent is the event type for events in Redis which will be
	// subscribed to using patterns.
	PatternEvent
)

type patternType uint8

const (
	patternPlain patternType = iota
	patternStar
	patternAlts
	patternPlaceholder
)

// SubCommand returns the command issued to subscribe to the event in Redis.
func (e EventType) SubCommand() string {
	switch e {
	case PlainEvent:
		return "SUBSCRIBE"
	case PatternEvent:
		return "PSUBSCRIBE"
	default:
		panic("unknown event type")
	}
}

// UnsubCommand returns the command issued
// o unsubscribe from the event in Redis.
func (e EventType) UnsubCommand() string {
	switch e {
	case PlainEvent:
		return "UNSUBSCRIBE"
	case PatternEvent:
		return "PUNSUBSCRIBE"
	default:
		panic("unknown event type")
	}
}

// Field is a type which is are concatenated into events, listed to over Redis.
type Field struct {
	valid   bool
	alias   string
	value   string
	pattern patternType
}

// IsZero returns true if the field is empty. A call to Event.Find() or
// Event.Get() with a non-existent alias or index will return such a struct.
func (f Field) IsZero() bool { return !f.valid }

// String returns the field value as a string.
func (f Field) String() string { return f.value }

// Bytes returns the field value as a byte slice.
func (f Field) Bytes() []byte { return []byte(f.value) }

// Int attempts to parse and return the field value as an integer.
func (f Field) Int() (int, error) {
	x, err := strconv.ParseInt(f.value, 10, 32)
	return int(x), err
}

// Uint64 attempts to parse and return the field value as a uint64.
func (f Field) Uint64() (uint64, error) { return strconv.ParseUint(f.value, 10, 64) }

// Int64 attempts to parse and return the field value as a int64.
func (f Field) Int64() (int64, error) { return strconv.ParseInt(f.value, 10, 64) }

// An Event is passed to an Emitter to manage which
// events a Listener is subscribed to.
type Event struct {
	fields []Field
	kind   EventType
}

// As sets the alias of the last field in the event list. You may then call
// Event.Find(alias) to look up the value of the field in the event.
func (e Event) As(alias string) Event {
	e.fields[len(e.fields)-1].alias = alias
	return e
}

// String creates a Field containing a string.
func (e Event) String(str string) Event {
	e.fields = append(e.fields, Field{valid: true, value: str})
	return e
}

// Int creates a Field containing an integer.
func (e Event) Int(x int) Event {
	e.fields = append(e.fields, Field{valid: true, value: strconv.Itoa(x)})
	return e
}

// Star creates a Field containing the Kleene star `*` for pattern subscription,
// and chains it on to the Event.
func (e Event) Star() Event {
	e.assertPattern()
	e.fields = append(e.fields, Field{valid: true, value: "*", pattern: patternStar})
	return e
}

// Placeholder creates a field containing a `?` for a placeholder in Redis patterns,
// and chains it on to the event.
func (e Event) Placeholder() Event {
	e.assertPattern()
	e.fields = append(e.fields, Field{valid: true, value: "?", pattern: patternPlaceholder})
	return e
}

// Alternatives creates a field with the alts wrapped in brackets, to match
// one of them in a Redis pattern, and chains it on to the event.
func (e Event) Alternatives(alts string) Event {
	e.assertPattern()
	e.fields = append(e.fields, Field{
		valid:   true,
		value:   "[" + alts + "]",
		pattern: patternAlts,
	})
	return e
}

// assertPattern panics if the event is not a Pattern type.
func (e Event) assertPattern() {
	if e.kind != PatternEvent {
		panic("That operation is only valid on pattern events created with NewPattern()")
	}
}

// Len returns the number of fields contained in the event.
func (e Event) Len() int { return len(e.fields) }

// Get returns the value of a field at index `i` within the event. If the
// field does not exist, an empty struct will be returned.
func (e Event) Get(i int) Field {
	if len(e.fields) <= i {
		return Field{valid: false}
	}

	return e.fields[i]
}

// Find looks up a field value by its alias. This is most useful in pattern
// subscriptions where might use Find to look up a parameterized property.
// If the alias does not exist, an empty struct will be returned.
func (e Event) Find(alias string) Field {
	for _, field := range e.fields {
		if field.alias == alias {
			return field
		}
	}

	return Field{valid: false}
}

// Name returns name of the event, formed by a concatenation of all the
// event fields.
func (e Event) Name() string {
	strs := make([]string, len(e.fields))
	for i, field := range e.fields {
		strs[i] = field.value
	}

	return strings.Join(strs, "")
}

// Type returns the type of the event (either a PlainEvent or a PatternEvent).
func (e Event) Type() EventType {
	return e.kind
}

func (e Event) concat(other Event) Event {
	e.fields = append(e.fields, other.fields...)
	return e
}

func (e Event) slice(start, end int) Event {
	e.fields = e.fields[start:end]
	return e
}

func (e Event) isZero() bool {
	return e.kind == 0
}

// applyFields attempts to convert the values from a string or byte slice into
// a Field. It panics if a value is none of the above.
func applyFields(event Event, values []interface{}) Event {
	for _, v := range values {
		switch t := v.(type) {
		case string:
			event = event.String(t)
		case []byte:
			event = event.String(string(t))
		default:
			panic(fmt.Sprintf("Expected string or field when creating an event, got %T", v))
		}
	}

	return event
}

// NewEvent creates and returns a new event based off the series of fields.
// This translates to a Redis SUBSCRIBE call.
func NewEvent(fields ...interface{}) Event {
	return applyFields(Event{kind: PlainEvent}, fields)
}

// NewPattern creates and returns a new event pattern off the series
// of fields. This translates to a Redis PSUBSCRIBE call.
func NewPattern(fields ...interface{}) Event {
	return applyFields(Event{kind: PatternEvent}, fields)
}

// Matches the special characters of an event against a specific channel,
// returning a new event that has the ambiguities (*, ?, [xy]) resolved.
// It returns a zero event if it cannot match the string.
//
// This is partly based on Redis' own matching from:
// https://github.com/antirez/redis/blob/unstable/src/util.c#L47
func matchPatternAgainst(ev Event, channel string) Event {
	if ev.kind != PatternEvent {
		panic("pattern matching is only valid against pattern events")
	}

	out := Event{
		fields: make([]Field, 0, len(ev.fields)),
		kind:   PatternEvent,
	}

	pos := 0
	chlen := len(channel)
	num := len(ev.fields)
	for i := 0; i < num; i++ {
		field := ev.fields[i]
		vallen := len(field.value)

		// Fail if the field is a pattern and the current character does not
		// contain one of the alternatives.
		if field.pattern == patternAlts && pos < chlen &&
			strings.IndexByte(field.value[1:vallen-1], channel[pos]) == -1 {
			return Event{}
		}

		// Swap out placeholders or alts for the next character, if there is one.
		if field.pattern == patternPlaceholder || field.pattern == patternAlts {
			if pos == chlen {
				return Event{}
			}
			out = out.String(string(channel[pos]))
			pos++
			continue
		}

		// Eat stars, yum
		if field.pattern == patternStar {
			// If this is the last component, eat the rest of the channel.
			if i == num-1 {
				out = out.String(channel[pos:])
				return out
			}

			// Start eating up the pattern until we get to a point where
			// we can match the rest. Then add what we ate as a string --
			// that's what the star contained -- and concat everything
			// else on the end.
			tail := ev.slice(i+1, num)
			for end := pos; end < chlen; end++ {
				tail := matchPatternAgainst(tail, channel[end:])
				if !tail.isZero() {
					out = out.String(channel[pos:end])
					return out.slice(0, i+1).concat(tail)
				}
			}

			// Can't find anything? This is invalid.
			return Event{}
		}

		// Otherwise it's a plain text match. Make sure it actually matches,
		// then add it on.
		if vallen+pos > chlen || channel[pos:pos+vallen] != field.value {
			return Event{}
		}

		out.fields = append(out.fields, field)
		pos += vallen
	}

	return out
}
