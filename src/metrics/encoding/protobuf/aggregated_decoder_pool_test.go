package protobuf

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3x/pool"
)

func TestAggregatedDecoderPool(t *testing.T) {
	opts := pool.NewObjectPoolOptions().SetSize(1)

	pool := NewAggregatedDecoderPool(opts)
	pool.Init()

	// Getting a decoder from pool.
	d := pool.Get().(*aggregatedDecoder)
	require.Empty(t, d.pb.Metric.TimedMetric.Id)
	require.Empty(t, cap(d.pb.Metric.TimedMetric.Id))
	d.pb.Metric.TimedMetric.Id = []byte("foo")

	// Close should reset the internal fields and put it back to pool.
	d.Close()
	require.Empty(t, len(d.pb.Metric.TimedMetric.Id))
	require.NotEmpty(t, cap(d.pb.Metric.TimedMetric.Id))

	// Get will return the previously used decoder.
	newDecoder := pool.Get().(*aggregatedDecoder)
	require.Empty(t, len(newDecoder.pb.Metric.TimedMetric.Id))
	require.NotEmpty(t, cap(newDecoder.pb.Metric.TimedMetric.Id))
}
