package pubsub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func publish(event string, data string) {
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()
	c.Do("publish", event, data)
}

func disrupt(client *Client) {
	client.tasks <- task{Action: disruptAction}
	client.WaitFor(ConnectedEvent)
}

func create(t *testing.T) *Client {
	client := New(ConnectionParam{
		Address: "127.0.0.1:6379",
		Timeout: time.Second,
	})
	go client.Connect()
	client.WaitFor(ConnectedEvent)

	return client
}

func TestBasic(t *testing.T) {
	client := create(t)
	defer client.TearDown()

	listener := client.Listener(Channel, "foobar")
	client.WaitFor(SubscribeEvent)
	publish("foobar", "heyo!")
	assert.Equal(t, "heyo!", string((<-listener.Messages).Data))
}

func TestReconnects(t *testing.T) {
	client := create(t)
	defer client.TearDown()

	listener := client.Listener(Channel, "foobar")
	client.WaitFor(SubscribeEvent)
	publish("foobar", "heyo!")
	assert.Equal(t, "heyo!", string((<-listener.Messages).Data))

	go func() {
		time.Sleep(10 * time.Millisecond)
		disrupt(client)
	}()

	client.WaitFor(SubscribeEvent)
	publish("foobar", "we're back!")
	assert.Equal(t, "we're back!", string((<-listener.Messages).Data))
}

func TestIncreasesReconnectTimeAndResets(t *testing.T) {
	client := create(t)
	defer client.TearDown()
	client.Listener(Channel, "foobar")

	for i, prev := 0, -1; i < 5; i++ {
		assert.True(t, time.Duration(prev) < client.policy.Next())
	}

	disrupt(client)

	assert.Equal(t, 0, int(client.policy.Next()))
}

func TestUnsubscribe(t *testing.T) {
	client := create(t)
	defer client.TearDown()

	listener := client.Listener(Channel, "foobar")
	client.WaitFor(SubscribeEvent)
	publish("foobar", "heyo!")
	assert.Equal(t, "heyo!", string((<-listener.Messages).Data))

	// Unsubscribe, then publish and listen for a second to make sure
	// the event doesn't come in.
	listener.Unsubscribe()
	client.WaitFor(UnsubscribeEvent)
	publish("foobar", "heyo!")

	select {
	case packet := <-client.OnChannel(AnyEvent):
		assert.Fail(t, fmt.Sprintf("Got 'some' packet after unsubscribe: %#v", packet))
	case <-time.After(time.Millisecond * 100):
	}

	disrupt(client)

	// Make sure we don't resubscribe after a disruption.
	publish("foobar", "heyo!")
	select {
	case packet := <-client.OnChannel(AnyEvent):
		assert.Fail(t, fmt.Sprintf("Got 'some' packet after unsubscribe reconnect: %#v", packet))
	case <-time.After(time.Millisecond * 100):
	}
}

func TestDoesNotReconnectAfterGracefulClose(t *testing.T) {
	client := create(t)
	defer client.TearDown()

	client.Listener(Channel, "foobar")

	reconnect := make(chan bool)
	go (func() {
		client.WaitFor(ConnectedEvent)
		reconnect <- true
	})()

	select {
	case <-reconnect:
		assert.Fail(t, "Client should not have reconnected.")
	case <-time.After(time.Millisecond * 100):
	}
}

func TestPatternConnects(t *testing.T) {
	client := create(t)
	defer client.TearDown()

	listener := client.Listener(Pattern, "foo:*:bar")
	client.WaitFor(SubscribeEvent)
	publish("foo:2:bar", "heyo!")
	assert.Equal(t, "heyo!", string((<-listener.PMessages).Data))

	// Make sure we don't resubscribe after a disruption.
	listener.Unsubscribe()
	client.WaitFor(UnsubscribeEvent)
	publish("foo:2:bar", "oh no!")
	select {
	case packet := <-client.OnChannel(AnyEvent):
		assert.Fail(t, fmt.Sprintf("Got 'some' packet after unsubscribe: %#v", packet))
	case <-time.After(time.Millisecond * 100):
	}
}
