# Changelog

## pubsub

### 2.0 (25-08-2015) rc

 * **Breaking Change**: New() now takes a ConnectionParam value rather than a pointer.
 * **Breaking Change**: GetState() now returns a uint8 rather than a user-defined type, for greater compatibility with [fsm](https://github.com/WatchBeam/fsm). <small>_mumble mumble generics_</small>
 * Fix potential data races on the internal subscription registry.
 * Fix potential data race resulting in subscription duplication during multiple reconnections.
 * Allow specification of connection timeout (deadlines).
 * Allow specification of reconnection policies.
 * Cause subscription, unsubscriptions, and teardowns to happen more quickly.
 * Improve events system for increased flexibility.
 * Significantly improve conciseness and speed.


### 1.1 (25-08-2015)

 * **Breaking Change**: New() now takes a *ConnectionParam struct as its first argument.
 * Add password authentication options (from @janeczku).
 * Fix failing tests in Go 1.3
 * Prevent paniking when tearing down a client which was not set up.


### 1.0 (07-04-2015)

Initial
