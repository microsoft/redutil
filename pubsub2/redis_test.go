package pubsub

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"unsafe"

	"github.com/garyburd/redigo/redis"
	"github.com/mixer/redutil/conn"
	"github.com/mixer/redutil/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type mockListener struct {
	mock.Mock
	called chan struct{}
}

func (m *mockListener) Handle(ev Event, b []byte) {
	m.Called(ev, b)
	m.called <- struct{}{}
}

func (m *mockListener) waitForCall() {
	select {
	case <-m.called:
	case <-time.After(time.Second):
		panic("expected to get a call to the listener")
	}
}

func newMockListener() *mockListener {
	return &mockListener{called: make(chan struct{}, 1)}
}

func newTestRecordList() (recs *RecordList, l1 Listener, l2 Listener) {
	recs = NewRecordList()
	l1 = newMockListener()
	l2 = newMockListener()

	recs.Add(NewEvent("foo"), l1)
	recs.Add(NewEvent("foo"), l2)

	return recs, l1, l2
}

func assertListenersEqual(t *testing.T, r *record, event EventBuilder, name string, listeners []Listener) {
	assert.Equal(t, r.ev, event)
	assert.Equal(t, r.name, name)
	assert.Equal(t, listeners, r.getList())
}

func TestRecordsAddListeners(t *testing.T) {
	list := NewRecordList()
	ev := NewEvent("foo")
	l1 := newMockListener()
	l2 := newMockListener()
	assert.Len(t, list.getList(), 0)

	list.Add(ev, l1)

	list1 := list.getList()
	assert.Len(t, list1, 1)
	assertListenersEqual(t, list1[0], ev, "foo", []Listener{l1})

	list.Add(ev, l2)

	list2 := list.getList()
	assert.Len(t, list2, 1)
	assertListenersEqual(t, list2[0], ev, "foo", []Listener{l2, l1})
}

func TestRecordsRemoves(t *testing.T) {
	recs, l1, l2 := newTestRecordList()
	ev := NewEvent("foo")
	assertListenersEqual(t, recs.getList()[0], ev, "foo", []Listener{l2, l1})
	recs.Remove(NewEvent("foo"), l2)
	assertListenersEqual(t, recs.getList()[0], ev, "foo", []Listener{l1})
	recs.Remove(NewEvent("foo"), l1)
	assert.Len(t, recs.getList(), 0)
}

func TestRecordFindCopyGetsEmptyByDefault(t *testing.T) {
	i, recs := NewRecordList().Find("foo")
	assert.Equal(t, -1, i)
	assert.Nil(t, recs)
}

func TestRecordsGetCopies(t *testing.T) {
	recs, l1, l2 := newTestRecordList()
	ev := NewEvent("foo")
	_, out := recs.Find("foo")
	l3 := newMockListener()

	originalList := out.getList()
	assert.Equal(t, originalList, []Listener{l2, l1})
	recs.Add(ev, l3)
	assert.Equal(t, originalList, []Listener{l2, l1})

	updatedList := out.getList()
	assert.Equal(t, updatedList, []Listener{l3, l2, l1})
	recs.Remove(ev, l1)
	recs.Remove(ev, l2)
	assert.Equal(t, updatedList, []Listener{l3, l2, l1})
	assert.Equal(t, originalList, []Listener{l2, l1})
	assert.Equal(t, out.getList(), []Listener{l3})
}

func doRandomly(fns []func()) {
	for _, i := range rand.Perm(len(fns)) {
		fns[i]()
	}
}

func TestRaceGuarantees(t *testing.T) {
	// this test will fail with the race detector enabled if anything that's
	// not thread-safe happens.

	recs := NewRecordList()
	ev1 := NewEvent("foo")
	ev2 := NewEvent("bar")
	until := time.Now().Add(500 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for time.Now().Before(until) {
			listener1 := newMockListener()
			listener2 := newMockListener()
			doRandomly([]func(){
				func() { recs.Add(ev1, listener1) },
				func() { recs.Add(ev2, listener1) },
				func() { recs.Add(ev1, listener2) },
				func() { recs.Add(ev2, listener2) },
			})

			doRandomly([]func(){
				func() { recs.Remove(ev1, listener1) },
				func() { recs.Remove(ev2, listener1) },
				func() { recs.Remove(ev1, listener2) },
				func() { recs.Remove(ev2, listener2) },
			})
		}
	}()

	go func() {
		defer wg.Done()
		for time.Now().Before(until) {
			recs.Find("bar")
			recs.Find("foo")
		}
	}()

	wg.Wait()
}

type RedisPubsubSuite struct {
	*test.RedisSuite
	emitter *Pubsub
}

func TestRedisPubsubSuite(t *testing.T) {
	pool, _ := conn.New(conn.ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	suite.Run(t, &RedisPubsubSuite{RedisSuite: test.NewSuite(pool)})
}

func (r *RedisPubsubSuite) SetupTest() {
	r.emitter = NewPubsub(r.Pool)
}

func (r *RedisPubsubSuite) TearDownTest() {
	r.emitter.Close()
}

// waitForSubscribers blocks until there are at least `num` subscribers
// listening on the `channel` in Redis.
func (r *RedisPubsubSuite) waitForSubscribers(channel string, num int) {
	cnx := r.Pool.Get()
	defer cnx.Close()

	for {
		list, err := redis.Values(cnx.Do("PUBSUB", "NUMSUB", channel))
		if err != nil {
			panic(err)
		}

		var count int
		if _, err := redis.Scan(list, nil, &count); err != nil {
			panic(err)
		}

		if count >= num {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (r *RedisPubsubSuite) TestBasicReception() {
	cnx := r.Pool.Get()
	defer cnx.Close()

	l := newMockListener()
	defer l.AssertExpectations(r.T())

	ev := NewEvent("foo")
	body := []byte("bar")
	r.emitter.Subscribe(ev, l)
	l.On("Handle", ev.ToEvent("foo", "foo"), body).Return()
	r.MustDo("PUBLISH", "foo", body)
	l.waitForCall()
}

func (r *RedisPubsubSuite) TestCallsMultipleListeners() {
	cnx := r.Pool.Get()
	defer cnx.Close()

	l1 := newMockListener()
	defer l1.AssertExpectations(r.T())
	l2 := newMockListener()
	defer l2.AssertExpectations(r.T())

	ev := NewEvent("foo")
	body1 := []byte("bar1")
	body2 := []byte("bar2")
	body3 := []byte("bar3")
	l1.On("Handle", ev.ToEvent("foo", "foo"), body1).Return()
	l2.On("Handle", ev.ToEvent("foo", "foo"), body1).Return()
	l2.On("Handle", ev.ToEvent("foo", "foo"), body2).Return()

	r.emitter.Subscribe(ev, l1)
	r.emitter.Subscribe(ev, l2)

	r.MustDo("PUBLISH", "foo", body1)
	l1.waitForCall()
	l2.waitForCall()

	r.emitter.Unsubscribe(ev, l1)

	r.MustDo("PUBLISH", "foo", body2)
	l2.waitForCall()

	r.emitter.Unsubscribe(ev, l2)
	r.MustDo("PUBLISH", "foo", body3)
}

func (r *RedisPubsubSuite) TestsCallsWithFancyPatterns() {
	cnx := r.Pool.Get()
	defer cnx.Close()

	l1 := newMockListener()
	defer l1.AssertExpectations(r.T())

	ev := NewPattern("foo:").Int(42).String(":bar")
	body1 := []byte("bar1")
	l1.On("Handle", ev.ToEvent("foo:42:bar", "foo:*:bar"), body1).Return()
	r.emitter.Subscribe(NewPattern().String("foo:").Star().String(":bar"), l1)

	r.MustDo("PUBLISH", "foo:42:bar", body1)
	l1.waitForCall()
}

func (r *RedisPubsubSuite) TestResubscribesWhenDies() {
	cnx := r.Pool.Get()
	defer cnx.Close()

	body := []byte("bar")
	ev := NewEvent("foo")
	l := newMockListener()
	defer l.AssertExpectations(r.T())

	r.emitter.Subscribe(ev, l)

	r.MustDo("CLIENT", "KILL", "SKIPME", "yes")
	select {
	case <-r.emitter.Errs():
	case <-time.After(time.Second):
		r.Fail("timeout: expected to get an error after kill Redis conns")
	}

	r.waitForSubscribers("foo", 1)
	l.On("Handle", ev.ToEvent("foo", "foo"), body).Return()
	r.MustDo("PUBLISH", "foo", body)
	l.waitForCall()
}

func createBenchmarkList(count int, removeEvery int) (listeners []*Listener, recordInst *record, recordList *RecordList) {
	listeners = make([]*Listener, count)
	for i := 0; i < count; i++ {
		wrapped := ListenerFunc(func(_ Event, _ []byte) {})
		listeners[i] = &wrapped
	}

	recordInst = &record{list: unsafe.Pointer(&listeners)}
	recordInner := []*record{recordInst}
	recordList = NewRecordList()
	recordList.list = unsafe.Pointer(&recordInner)

	for i := removeEvery; i < count; i += removeEvery {
		recordList.Remove(NewEvent(), *listeners[i])
	}

	return
}

func runBenchmarkAddBenchmark(count int, b *testing.B) {
	listeners, recordInst, recordList := createBenchmarkList(count, 3)
	b.ResetTimer()

	ev := NewEvent()
	fn := ListenerFunc(func(_ Event, _ []byte) {})
	for i := 0; i < b.N; i++ {
		recordInst.list = unsafe.Pointer(&listeners)
		recordList.Add(ev, fn)
	}
}

func runBenchmarkRemoveBenchmark(count int, b *testing.B) {
	listeners, recordInst, recordList := createBenchmarkList(count, 3)
	first := listeners[0]
	b.ResetTimer()

	ev := NewEvent()
	for i := 0; i < b.N; i++ {
		listeners[0] = first
		recordInst.list = unsafe.Pointer(&listeners)
		recordList.Remove(ev, *first)
	}
}

func runBenchmarkBroadcastBenchmark(count int, b *testing.B) {
	_, recordInst, _ := createBenchmarkList(count, 3)
	b.ResetTimer()

	ev := NewEvent().ToEvent("", "")
	for i := 0; i < b.N; i++ {
		recordInst.Emit(ev, nil)
	}
}

func BenchmarkBroadcast1K(b *testing.B)   { runBenchmarkBroadcastBenchmark(1000, b) }
func BenchmarkBroadcast10K(b *testing.B)  { runBenchmarkBroadcastBenchmark(10000, b) }
func BenchmarkBroadcast100K(b *testing.B) { runBenchmarkBroadcastBenchmark(100000, b) }

func BenchmarkRecordAdd1K(b *testing.B)   { runBenchmarkAddBenchmark(1000, b) }
func BenchmarkRecordAdd10K(b *testing.B)  { runBenchmarkAddBenchmark(10000, b) }
func BenchmarkRecordAdd100K(b *testing.B) { runBenchmarkAddBenchmark(100000, b) }

func BenchmarkRecordRemove1K(b *testing.B)   { runBenchmarkRemoveBenchmark(1000, b) }
func BenchmarkRecordRemove10K(b *testing.B)  { runBenchmarkRemoveBenchmark(10000, b) }
func BenchmarkRecordRemove100K(b *testing.B) { runBenchmarkRemoveBenchmark(100000, b) }
