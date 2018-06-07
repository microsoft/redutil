package pubsub

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/garyburd/redigo/redis"
)

// compactionThreshold defines how many inactiveItems / total length
// have to be in the record before we'll do a full cleanup sweep.
const compactionThreshold = 0.5

type record struct {
	name          string
	ev            EventBuilder
	list          unsafe.Pointer // to a []Listener
	inactiveItems int
}

// Emit invokes all attached listeners with the provided event.
func (r *record) Emit(ev Event, b []byte) {
	// This looks absolutely _horrible_. It'd be better in C. Anyway, the idea
	// is that we have a list of unsafe pointers, which is stored in the list
	// which is an atomic pointer itself. We can swap the pointer held at each
	// address in the list, so _that_ address needs to be loaded atomically.
	// This means we need to actually load the data from the address at
	// offset X from the start of the array underlying the slice.
	//
	// The cost of the atomic loading makes 5-10% slower, but allows us to
	// atomically insert and remove listeners in many cases.

	list := r.getUnsafeList()
	for i := 0; i < len(list); i++ {
		addr := uintptr(unsafe.Pointer(&list[i]))
		value := atomic.LoadPointer(*(**unsafe.Pointer)(unsafe.Pointer(&addr)))
		if (uintptr)(value) != 0 {
			(*(*Listener)(value)).Handle(ev, b)
		}
	}
}

func (r *record) setList(l []unsafe.Pointer) { atomic.StorePointer(&r.list, (unsafe.Pointer)(&l)) }

func (r *record) storeAtIndex(index int, listener Listener) {
	// see Emit for details about this code.
	list := r.getUnsafeList()
	addr := uintptr(unsafe.Pointer(&list[index]))

	var storedPtr unsafe.Pointer
	if listener != nil {
		storedPtr = unsafe.Pointer(&listener)
	}

	atomic.StorePointer(*(**unsafe.Pointer)(unsafe.Pointer(&addr)), storedPtr)
}

func (r *record) getList() []Listener {
	original := r.getUnsafeList()
	output := make([]Listener, len(original))
	for i, ptr := range original {
		if (uintptr)(ptr) != 0 {
			output[i] = *(*Listener)(ptr)
		}
	}

	return output
}

func (r *record) getUnsafeList() []unsafe.Pointer {
	return *(*[]unsafe.Pointer)(atomic.LoadPointer(&r.list))
}

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
		rec.setList([]unsafe.Pointer{unsafe.Pointer(&fn)})
		r.append(rec)
		return 1
	}

	oldList := rec.getUnsafeList()
	newCount := len(oldList) - rec.inactiveItems + 1
	if rec.inactiveItems == 0 {
		newList := make([]unsafe.Pointer, len(oldList)+1)
		copy(newList, oldList)
		newList[len(oldList)] = unsafe.Pointer(&fn)
		rec.setList(newList)
		return newCount
	}

	for i, ptr := range oldList {
		if (uintptr)(ptr) == 0 {
			rec.storeAtIndex(i, fn)
			return newCount
		}
	}

	return newCount
}

// Remove delete the listener from the event. Returns the event's remaining
// listeners. Not thread-safe with other write operations.
func (r *recordList) Remove(ev EventBuilder, fn Listener) int {
	idx, rec := r.Find(ev.Name())
	if idx == -1 {
		return 0
	}

	// 1. Find the index of the listener in the list
	oldList := rec.getUnsafeList()
	spliceIndex := -1
	for i, l := range oldList {
		if (uintptr)(l) != 0 && (*(*Listener)(l)) == fn {
			spliceIndex = i
			break
		}
	}

	if spliceIndex == -1 {
		return len(oldList)
	}

	newCount := len(oldList) - rec.inactiveItems - 1
	// 2. If that's the only listener, just remove that record entirely.
	if newCount == 0 {
		r.remove(idx)
		return 0
	}

	// 3. Otherwise, wipe the pointer, or make a new list copied from the parts
	// of the old if we wanted to compact it.
	if float32(rec.inactiveItems+1)/float32(len(oldList)) < compactionThreshold {
		rec.storeAtIndex(spliceIndex, nil)
		rec.inactiveItems++
		return newCount
	}

	newList := make([]unsafe.Pointer, 0, newCount)
	for i, ptr := range oldList {
		if (uintptr)(ptr) != 0 && i != spliceIndex {
			newList = append(newList, ptr)
		}
	}
	rec.inactiveItems = 0
	rec.setList(newList)

	return newCount
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
