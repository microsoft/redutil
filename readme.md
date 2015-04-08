# redutil [![Build Status](https://travis-ci.org/MCProHosting/redutil.svg?branch=master)](https://travis-ci.org/MCProHosting/redutil) [![Coverage Status](https://coveralls.io/repos/MCProHosting/redutil/badge.svg?branch=master)](https://coveralls.io/r/MCProHosting/redutil?branch=master) [![godoc reference](https://godoc.org/github.com/mcprohosting/redutil?status.png)](https://godoc.org/github.com/MCProHosting/redutil/pubsub)


This package consists of several utilities to make Redis easier and more consistent in Go.

## pubsub

Tranditional Redis libraries allow you to subscribe to events, and maybe even pool connections. But there's often no mechanism for maintaining subscribed state in the event of a connection failure, and many packages aren't thread-safe. This package, `redutil/pubsub`, solves these issues.

It is fully thread safe and unit tested. We're currently using it in production, though it has not yet been entirely battle-tested. Feel free to open issues on this repository.

```go
package main

import (
    "time"
    "github.com/mcprohosting/redutil/pubsub"
)

func main() {
    // Create a new pubsub client. This will create and manage connections,
    // even if you disconnect.
    c := pubsub.New("127.0.0.1:6379")
    defer c.TearDown()

    go listenChannel(c)
    go listenPattern(c)

    // Wait forever!
    select {}
}


// Simple example function that listens for all events broadcast
// in the channel "chan".
func listenChannel(c *pubsub.Client) {
    listener := c.Listen(pubsub.Channel, "chan")
    defer listener.Unsubscribe()
    for _, message := range listener.Messages {
        doStuff()
    }
}

// Example that listens for events that match the pattern
// "foo:*:bar". Note that we listen to the `PMessages` channel, not `Messages`.
func listenPattern(c *pubsub.Client) {
    listener := c.Listen(pubsub.Pattern, "foo:*:bar")
    defer listener.Unsubscribe()

    for _, message := range listener.PMessages {
        // You got mail!
    }
}
```

## License

Copyright 2015 by Beam LLC. Distributed under the MIT license.
