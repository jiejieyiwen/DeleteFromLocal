package server

import "github.com/prometheus/client_golang/prometheus"

func init() {
	prometheus.MustRegister(_RecordDeleteTotal, _RecordDeleteReqDur)
}

var (
	_RecordDeleteTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "grpc_server",
		Subsystem: "record_delete",
		Name:      "total",
		Help:      "grpc server record delete count.",
	},
		[]string{"MountedPoint"},
	)
	_RecordDeleteReqDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "grpc_server",
		Subsystem: "record_delete",
		Name:      "duration_s",
		Help:      "grpc server record delete duration(ms).",
		Buckets:   []float64{100, 500, 1000, 2000, 3000, 4000, 5000},
	},
		[]string{"MountedPoint"},
	)
)
