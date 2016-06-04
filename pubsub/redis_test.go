package pubsub

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"

	"github.com/WatchBeam/redutil/conn"
	"github.com/WatchBeam/redutil/test"
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

func newTestRecordList() (recs *recordList, l1 Listener, l2 Listener) {
	recs = &recordList{}
	l1 = newMockListener()
	l2 = newMockListener()

	recs.Add(NewEvent("foo"), l1)
	recs.Add(NewEvent("foo"), l2)

	return recs, l1, l2
}

func TestRecordsAddListeners(t *testing.T) {
	list := &recordList{}
	ev := NewEvent("foo")
	l1 := newMockListener()
	l2 := newMockListener()
	assert.Len(t, list.list, 0)

	list.Add(ev, l1)

	assert.Len(t, list.list, 1)
	assert.Equal(t,
		record{ev: ev, name: "foo", list: []Listener{l1}},
		*list.list[0],
	)

	list.Add(ev, l2)

	assert.Len(t, list.list, 1)
	assert.Equal(t,
		record{ev: ev, name: "foo", list: []Listener{l1, l2}},
		*list.list[0],
	)
}

func TestRecordsRemoves(t *testing.T) {
	recs, l1, l2 := newTestRecordList()
	assert.Len(t, recs.list[0].list, 2)
	recs.Remove(NewEvent("foo"), l2)
	assert.Len(t, recs.list[0].list, 1)
	recs.Remove(NewEvent("foo"), l1)
	assert.Len(t, recs.list, 0)
}

func TestRecordFindCopyGetsEmptyByDefault(t *testing.T) {
	recs := (&recordList{}).FindCopy("foo")
	assert.Len(t, recs.list, 0)
}

func TestRecordsGetCopies(t *testing.T) {
	recs, _, _ := newTestRecordList()
	out := recs.FindCopy("foo")
	assert.Len(t, out.list, 2)
	recs.list[0].list = nil
	assert.Len(t, out.list, 2)
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
