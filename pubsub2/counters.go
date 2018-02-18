package pubsub

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	PromSubscriptions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redutil_pubsub_subscriptions",
		Help: "Number of subscriptions held by Redutil",
	})
	PromReconnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redutil_pubsub_reconnections",
		Help: "Number of times pubsub has reconnected",
	})
	PromSendLatency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redutil_pubsub_send_latency",
		Help: "Amount of time it last took to fire a prometheus event",
	})
	PromReconnectLatency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redutil_pubsub_reconnect_latency",
		Help: "Amount of time it last took to reconnect",
	})
	PromSubLatency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redutil_pubsub_sub_latency",
		Help: "Amount of time it last took to subscribe or unsubscribe",
	})
)

func gaugeLatency(g prometheus.Gauge) (stop func()) {
	start := time.Now()

	return func() {
		g.Set(float64(time.Now().Sub(start)) / float64(time.Millisecond))
	}
}
