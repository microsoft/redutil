package pubsub

import (
	"github.com/WatchBeam/fsm"
)

const (
	// Not currently connected to a server.
	DisconnectedState uint8 = iota
	// Connected to a server, but not yet linked as a pubsub client.
	ConnectedState
	// We're in the process of closing the client.
	ClosingState
	// We were connected, but closed gracefully.
	ClosedState
)

var blueprint *fsm.Blueprint

func init() {
	bp := fsm.New()
	bp.Start(DisconnectedState)
	bp.From(DisconnectedState).To(ConnectedState)
	bp.From(DisconnectedState).To(ClosingState)
	bp.From(ConnectedState).To(DisconnectedState)
	bp.From(ConnectedState).To(ClosingState)
	bp.From(ClosingState).To(ClosedState)

	blueprint = bp
}
