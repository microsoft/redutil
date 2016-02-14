package heartbeat

// Detector is an interface to a type responsible for collecting "dead" items in
// Redis.
type Detector interface {
	// Detect returns all dead items according to the implementation
	// definition of "dead". If an error is encountered while processing, it
	// will be returned, and execution of this function will be halted.
	Detect() (expired []string, err error)

	// Removes a (supposedly dead) worker from the heartbeating data structure.
	Purge(id string) error
}
