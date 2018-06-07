package fuzz

import (
	"fmt"

	"github.com/mixer/redutil/pubsub2"
)

var (
	listeners = []pubsub.Listener{}
	event     = pubsub.NewEvent()
)

func init() {
	for i := 0; i <= 0xFF; i++ {
		listeners = append(listeners, pubsub.ListenerFunc(func(_ pubsub.Event, _ []byte) {}))
	}
}

// Fuzz is the main function for go-fuzz: https://github.com/dvyukov/go-fuzz.
// It adds and remove users according to the sequence of bytes and ensures
// state is consistent at the end.
func Fuzz(data []byte) int {
	list := pubsub.NewRecordList()
	isAdded := map[pubsub.Listener]bool{}
	for i := 0; i < len(data); i++ {
		listener := listeners[data[i]]
		if isAdded[listener] {
			list.Remove(event, listener)
			isAdded[listener] = false
		} else {
			list.Add(event, listener)
			isAdded[listener] = true
		}
	}

	result := list.ListenersFor(event)
	for i, l := range listeners {
		exists := false
		for _, l2 := range result {
			if l2 == l {
				exists = true
				break
			}
		}

		if exists != isAdded[l] {
			panic(fmt.Sprintf("expected listener %d exists=%+v, but it was not", i, isAdded[l]))
		}
	}

	return 0
}
