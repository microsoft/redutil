package pubsub

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/garyburd/redigo/redis"
)

type record struct {
	name string
	ev   EventBuilder
	list unsafe.Pointer // to a []Listener
}

// Emit invokes all attached listeners with the provided event.
func (r *record) Emit(ev Event, b []byte) {
	for _, l := range r.getList() {
		l.Handle(ev, b)
	}
}

func (r *record) setList(l []Listener) { atomic.StorePointer(&r.list, (unsafe.Pointer)(&l)) }
func (r *record) getList() []Listener  { return *(*[]Listener)(atomic.LoadPointer(&r.list)) }

type recordList struct {
	list unsafe.Pointer // to a []*record
}

func newRecordList() *recordList {
	init := []*record{}
	return &recordList{unsafe.Pointer(&init)}
}

// Find looks up the index for the record corresponding to the provided
// event name. It returns -1 if one was not found. Thread-safe.
func (r *recordList) Find(ev string) (index int, rec *record) {
	for i, rec := range r.getList() {
		if rec.name == ev {
			return i, rec
		}
	}

	return -1, nil
}

// Add inserts a new listener for an event. Returns the incremented
// number of listeners. Not thread-safe with other write operations.
func (r *recordList) Add(ev EventBuilder, fn Listener) int {
	idx, rec := r.Find(ev.Name())
	if idx == -1 {
		rec := &record{ev: ev, name: ev.Name()}
		rec.setList([]Listener{fn})
		r.append(rec)
		return 1
	}

	oldList := rec.getList()
	newList := make([]Listener, len(oldList)+1)
	copy(newList, oldList)
	newList[len(oldList)] = fn
	rec.setList(newList)

	return len(newList)
}

// Remove delete the listener from the event. Returns the event's remaining
// listeners. Not thread-safe with other write operations.
func (r *recordList) Remove(ev EventBuilder, fn Listener) int {
	idx, rec := r.Find(ev.Name())
	if idx == -1 {
		return 0
	}

	// 1. Find the index of the listener in the list
	oldList := rec.getList()
	spliceIndex := -1
	for i, l := range oldList {
		if l == fn {
			spliceIndex = i
			break
		}
	}

	if spliceIndex == -1 {
		return len(oldList)
	}

	// 2. If that's the only listener, just remove that record entirely.
	if len(oldList) == 1 {
		r.remove(idx)
		return 0
	}

	// 3. Otherwise, make a new list copied from the parts of the old
	newList := make([]Listener, len(oldList)-1)
	copy(newList, oldList[:spliceIndex])
	copy(newList[spliceIndex:], oldList[spliceIndex+1:])
	rec.setList(newList)

	return len(newList)
}

func (r *recordList) setList(l []*record) { atomic.StorePointer(&r.list, (unsafe.Pointer)(&l)) }

func (r *recordList) getList() []*record { return *(*[]*record)(atomic.LoadPointer(&r.list)) }

func (r *recordList) append(rec *record) {
	oldList := r.getList()
	newList := make([]*record, len(oldList)+1)
	copy(newList, oldList)
	newList[len(oldList)] = rec
	r.setList(newList)
}

func (r *recordList) remove(index int) {
	oldList := r.getList()
	newList := make([]*record, len(oldList)-1)
	copy(newList, oldList[:index])
	copy(newList[index:], oldList[index+1:])
	r.setList(newList)
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
			PlainEvent:   newRecordList(),
			PatternEvent: newRecordList(),
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
			go write.Work()
			p.resubscribe(write)

			go read.Work()
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
func (p *Pubsub) resubscribe(write *writePump) {
	timer := gaugeLatency(PromReconnectLatency)
	PromReconnections.Inc()

	p.subsMu.Lock()
	defer p.subsMu.Unlock()

	for kind, recs := range p.subs {
		if recs == nil {
			continue
		}

		for _, ev := range recs.getList() {
			write.Data() <- command{
				command: EventType(kind).SubCommand(),
				channel: ev.name,
			}
		}
	}

	timer()
}

func (p *Pubsub) handleEvent(data interface{}) {
	timer := gaugeLatency(PromSendLatency)
	defer timer()

	switch t := data.(type) {
	case redis.Message:
		_, rec := p.subs[PlainEvent].Find(t.Channel)
		if rec == nil {
			return
		}

		rec.Emit(rec.ev.ToEvent(t.Channel, t.Channel), t.Data)

	case redis.PMessage:
		_, rec := p.subs[PatternEvent].Find(t.Pattern)
		if rec == nil {
			return
		}

		match, ok := matchPatternAgainst(rec.ev, t.Channel)
		if !ok {
			rec.Emit(rec.ev.ToEvent(t.Channel, t.Pattern), t.Data)
		} else {
			rec.Emit(match.ToEvent(t.Channel, t.Pattern), t.Data)
		}
	}
}

// Errs implements Emitter.Errs
func (p *Pubsub) Errs() <-chan error {
	return p.errs
}

// Subscribe implements Emitter.Subscribe
func (p *Pubsub) Subscribe(ev EventBuilder, l Listener) {
	timer := gaugeLatency(PromSubLatency)
	defer timer()

	p.subsMu.Lock()
	count := p.subs[ev.kind].Add(ev, l)
	p.subsMu.Unlock()

	if count == 1 {
		PromSubscriptions.Inc()
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
	timer := gaugeLatency(PromSubLatency)
	defer timer()

	p.subsMu.Lock()
	count := p.subs[ev.kind].Remove(ev, l)
	p.subsMu.Unlock()

	if count == 0 {
		PromSubscriptions.Dec()
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
