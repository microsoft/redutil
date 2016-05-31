package pubsub

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventTypes(t *testing.T) {
	assert.Equal(t, "SUBSCRIBE", PlainEvent.SubCommand())
	assert.Equal(t, "UNSUBSCRIBE", PlainEvent.UnsubCommand())
	assert.Equal(t, "PSUBSCRIBE", PatternEvent.SubCommand())
	assert.Equal(t, "PUNSUBSCRIBE", PatternEvent.UnsubCommand())
}

func TestEventBuildsString(t *testing.T) {
	e := NewEvent("foo")
	assert.Equal(t, PlainEvent, e.Type())
	assert.Equal(t, e.Name(), "foo")
}

func TestEventBuildsPattern(t *testing.T) {
	e := NewPatternEvent("foo")
	assert.Equal(t, PatternEvent, e.Type())
	assert.Equal(t, e.Name(), "foo")
}

func TestEventBuildsMultipart(t *testing.T) {
	e := NewEvent("prefix:", String("foo:"), Int(42))
	assert.Equal(t, "prefix:foo:42", e.Name())
	assert.Equal(t, 3, e.Len())

	assert.Equal(t, "prefix:", e.Get(0).String())
	id, _ := e.Get(2).Int()
	assert.Equal(t, "foo:", e.Get(1).String())
	assert.Equal(t, 42, id)
}

func TestEventReturnsZeroOnDNE(t *testing.T) {
	assert.True(t, NewEvent("foo").Get(1).IsZero())
	assert.False(t, NewEvent("foo").Get(0).IsZero())
	assert.True(t, NewEvent("foo", Int(1).As("bar")).Find("bleh").IsZero())
	assert.False(t, NewEvent("foo", Int(1).As("bar")).Find("bar").IsZero())
}

func TestEventMatchesPattern(t *testing.T) {
	tt := []struct {
		isMatch bool
		event   Event
		channel string
	}{
		{true, NewPatternEvent("foo"), "foo"},
		{false, NewPatternEvent("foo"), "bar"},
		{false, NewPatternEvent("fooo"), "foo"},
		{false, NewPatternEvent("foo"), "fooo"},

		{true, NewPatternEvent("foo", Star()), "foo"},
		{true, NewPatternEvent("foo", Star()), "fooasdf"},
		{true, NewPatternEvent("foo", Star(), String("bar")), "foo42bar"},
		{false, NewPatternEvent("foo", Star(), String("nar")), "foo42bar"},
		{true, NewPatternEvent("foo", Star(), String("bar"), Star()), "foo42bar"},
		{true, NewPatternEvent("foo", Star(), String("bar"), Star()), "foo42bar42"},
		{false, NewPatternEvent("foo", Star(), String("baz"), Star()), "foo42bar42"},

		{false, NewPatternEvent("foo", Alternatives("123")), "foo6"},
		{true, NewPatternEvent("foo", Alternatives("123")), "foo2"},
	}

	for _, test := range tt {
		actual := matchPatternAgainst(test.event, test.channel).Name()
		matches := test.channel == actual
		if test.isMatch {
			assert.True(t, matches, fmt.Sprintf("%s ∉ %s", test.channel, test.event.Name()))
		} else {
			assert.False(t, matches, fmt.Sprintf("%s ∈ %s", test.channel, test.event.Name()))
		}
	}
}

func TestFuzz(t *testing.T) {
	if os.Getenv("FUZZ") == "" {
		return
	}

	fields := []struct {
		Field    Field
		Matching string
	}{
		{Star(), "adsf"},
		{String("foo"), "foo"},
		{String("bar"), "bar"},
		{Alternatives("123"), "2"},
	}
	// Transition matrix for fields, by index. Given an [x, y], field[y] has
	// a matrix[x][y] chance of transitioning into field[x] next
	transitions := [][]float64{
		{0, 0.34, 0.34, 0.32},
		{0.3, 0.2, 0.3, 0.2},
		{0.3, 0.3, 0.2, 0.2},
		{0.25, 0.25, 0.25, 0.25},
	}

	fmt.Println("")

	for k := 0; true; k++ {
		list := []Field{}
		matching := ""

		// 1. build the event
		for i := rand.Intn(len(fields)); len(list) == 0 || rand.Float64() < 0.8; {
			list = append(list, fields[i].Field)
			matching += fields[i].Matching

			x := rand.Float64()
			sum := float64(0)
			for idx, p := range transitions[i] {
				sum += p
				if x < sum {
					i = idx
					break
				}
			}
		}

		event := NewPatternEvent(list[0], list[1:]...)
		if matching != matchPatternAgainst(event, matching).Name() {
			panic(fmt.Sprintf("%s ∉ %s", matching, event.Name()))
		}

		if k%100000 == 0 {
			fmt.Printf("\033[2K\r%d tests run -- %s ∈ %s", k, matching, event.Name())
		}
	}
}
