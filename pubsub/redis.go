package pubsub

import (
	"sync"

	"github.com/garyburd/redigo/redis"
)

type record struct {
	name string
	ev   EventBuilder
	list []Listener
}

// Emit invokes all attached listeners with the provided event.
func (r *record) Emit(ev Event, b []byte) {
	for _, l := range r.list {
		l.Handle(ev, b)
	}
}

type recordList struct{ list []*record }

// FindCopy looks up an record in the list by the event name and returns
// a copy of it. This is done so that the copy may be used without needing
// to maintain the lock on the record list.
func (r *recordList) FindCopy(ev string) *record {
	_, rec := r.find(ev)
	if rec == nil {
		return &record{}
	}

	dup := &record{
		ev:   rec.ev,
		name: rec.name,
		list: make([]Listener, len(rec.list)),
	}

	copy(dup.list, rec.list)

	return dup
}

// findIndex looks up the index for the record corresponding to the provided
// event name. It returns -1 if one was not found.
func (r *recordList) find(ev string) (index int, rec *record) {
	for i, rec := range r.list {
		if rec.name == ev {
			return i, rec
		}
	}

	return -1, nil
}

// Add inserts a new listener for an event. Returns the incremented
// number of listeners.
func (r *recordList) Add(ev EventBuilder, fn Listener) int {
	idx, rec := r.find(ev.Name())
	if idx == -1 {
		r.list = append(r.list, &record{
			ev:   ev,
			name: ev.Name(),
			list: []Listener{fn},
		})
		return 1
	}

	rec.list = append(rec.list, fn)
	return len(rec.list)
}

// Remove delete the listener from the event. Returns the event's remaining
// listeners.
func (r *recordList) Remove(ev EventBuilder, fn Listener) int {
	idx, rec := r.find(ev.Name())
	if idx == -1 {
		return 0
	}

	for i, l := range rec.list {
		if l == fn {
			// Annoying cut since the Listener is a pointer
			rec.list[i] = rec.list[len(rec.list)-1]
			rec.list[len(rec.list)-1] = nil
			rec.list = rec.list[:len(rec.list)-1]
			break
		}
	}

	if l := len(rec.list); l > 0 {
		return l
	}

	// More annoying cuts for pointers
	r.list[idx] = r.list[len(r.list)-1]
	r.list[len(r.list)-1] = nil
	r.list = r.list[:len(r.list)-1]
	return 0
}

// Pubsub is an implementation of the Emitter interface using
// Redis pupsub.
type Pubsub struct {
	pool   *redis.Pool
	errs   chan error
	closer chan struct{}
	send   chan command

	// Lists of listeners for subscribers and pattern subscribers
	subsMu sync.Mutex
	subs   []*recordList
}

// NewPubsub creates a new Emitter based on pubsub on the provided
// Redis pool.
func NewPubsub(pool *redis.Pool) *Pubsub {
	ps := &Pubsub{
		pool:   pool,
		errs:   make(chan error),
		closer: make(chan struct{}),
		send:   make(chan command),
		subs: []*recordList{
			PlainEvent:   &recordList{},
			PatternEvent: &recordList{},
		},
	}

	go ps.work()

	return ps
}

var _ Emitter = new(Pubsub)

// Inner working loop for the emitter, runs until .Close() is called.
func (p *Pubsub) work() {
	var (
		cnx   redis.Conn
		read  *readPump
		write *writePump
	)

	teardown := func() {
		read.Close()
		write.Close()
		cnx.Close()
		cnx = nil
	}

	defer teardown()

	for {
		if cnx == nil {
			cnx = p.pool.Get()
			read = newReadPump(cnx)
			write = newWritePump(cnx)
			p.resubscribe()

			go read.Work()
			go write.Work()
		}

		select {
		case <-p.closer:
			return
		case data := <-p.send:
			write.Data() <- data
			data.written <- struct{}{}
		case event := <-read.Data():
			go p.handleEvent(event)
		case err := <-read.Errs():
			teardown()
			p.errs <- err
		case err := <-write.Errs():
			teardown()
			p.errs <- err
		}
	}
}

// resubscribe flushes the `send` queue and replaces it with commands
// to resubscribe to all previously-subscribed-to channels. This will
// NOT block until all subs are resubmitted, only until we get a lock.
func (p *Pubsub) resubscribe() {
	p.subsMu.Lock()

	go func() {
		// Drain the "sent" channels. Channels added to the sent channel are
		// already recorded in the list and we don't need to dupe them. Do
		// store the callback, however.
		toWrite := map[string]chan<- struct{}{}
	L:
		for {
			select {
			case s := <-p.send:
				toWrite[s.channel] = s.written
			default:
				break L
			}
		}

		for kind, recs := range p.subs {
			if recs == nil {
				continue
			}

			for _, ev := range recs.list {
				var ch chan<- struct{}
				if existing, ok := toWrite[ev.name]; ok {
					ch = existing
				} else {
					ch = make(chan struct{}, 1)
				}

				p.send <- command{
					command: EventType(kind).SubCommand(),
					channel: ev.name,
					written: ch,
				}
			}
		}

		p.subsMu.Unlock()
	}()
}

func (p *Pubsub) handleEvent(data interface{}) {
	switch t := data.(type) {
	case redis.Message:
		p.subsMu.Lock()
		rec := p.subs[PlainEvent].FindCopy(t.Channel)
		p.subsMu.Unlock()
		rec.Emit(rec.ev.toEvent(t.Channel, t.Channel), t.Data)

	case redis.PMessage:
		p.subsMu.Lock()
		rec := p.subs[PatternEvent].FindCopy(t.Pattern)
		p.subsMu.Unlock()
		match, ok := matchPatternAgainst(rec.ev, t.Channel)

		if !ok {
			rec.Emit(rec.ev.toEvent(t.Channel, t.Pattern), t.Data)
		} else {
			rec.Emit(match.toEvent(t.Channel, t.Pattern), t.Data)
		}
	}
}

// Errs implements Emitter.Errs
func (p *Pubsub) Errs() <-chan error {
	return p.errs
}

// Subscribe implements Emitter.Subscribe
func (p *Pubsub) Subscribe(ev EventBuilder, l Listener) {
	p.subsMu.Lock()
	count := p.subs[ev.kind].Add(ev, l)
	p.subsMu.Unlock()

	if count == 1 {
		written := make(chan struct{}, 1)
		p.send <- command{
			command: ev.kind.SubCommand(),
			channel: ev.Name(),
			written: written,
		}

		<-written
	}
}

// Unsubscribe implements Emitter.Unsubscribe
func (p *Pubsub) Unsubscribe(ev EventBuilder, l Listener) {
	p.subsMu.Lock()
	count := p.subs[ev.kind].Remove(ev, l)
	p.subsMu.Unlock()

	if count == 0 {
		written := make(chan struct{}, 1)
		p.send <- command{
			command: ev.kind.UnsubCommand(),
			channel: ev.Name(),
			written: written,
		}

		<-written
	}
}

// Close implements Emitter.Close
func (p *Pubsub) Close() {
	p.closer <- struct{}{}
}
