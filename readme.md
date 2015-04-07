# redutil

This package consists of several utilities to make Redis more easier and consistent in Go.

## pubsub

Tranditional Redis libraries allow you to subscribe to events, and maybe even pool connections. But there's often no mechanism for maintaining subscribed state in the event of a connection failure. This package, `redutil/pubsub`, solves that issue.

It is fully thread safe and unit tested. We're currently using it in production, though it has not yet been entire battle-tested. Feel free to open issues on this repository.

```go
package main

import (
    "time"
    "github.com/mcprohosting/redutil/pubsub"
)

func main() {
    client := pubsub.New("127.0.0.1:6379")

    go func() {
        // Let's listen for channels!
        listener := client.Listen(pubsub.Pattern, "chanchan")
        for _, message := <- listener.PMessages {
            // You got mail!
        }
        listener.Unsubscribe()
    }()

    go func() {
        // As well as patterns!
        listener := client.Listen(pubsub.Pattern, "foo:*:bar")
        for _, message := <- listener.PMessages {
            // You got mail!
        }
        listener.Unsubscribe()
    }()

    time.Sleep(10 * time.Seconds)
    client.TearDown()
}

```
