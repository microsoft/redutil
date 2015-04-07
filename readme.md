# redutil [![Build Status](https://travis-ci.org/MCProHosting/redutil.svg?branch=master)](https://travis-ci.org/MCProHosting/redutil) [![Coverage Status](https://coveralls.io/repos/MCProHosting/redutil/badge.svg?branch=master)](https://coveralls.io/r/MCProHosting/redutil?branch=master) [![godoc reference](https://godoc.org/github.com/mcprohosting/redutil?status.png)][godoc]


This package consists of several utilities to make Redis more easier and consistent in Go.

## pubsub

Tranditional Redis libraries allow you to subscribe to events, and maybe even pool connections. But there's often no mechanism for maintaining subscribed state in the event of a connection failure. This package, `redutil/pubsub`, solves that issue.

It is fully thread safe and unit tested. We're currently using it in production, though it has not yet been entirely battle-tested. Feel free to open issues on this repository.

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
        listener := client.Listen(pubsub.Channel, "chanchan")
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
