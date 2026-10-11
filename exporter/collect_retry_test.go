package exporter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hsn723/postfix_exporter/logsource"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

type scriptedRead struct {
	err  error
	line string
}

// scriptedLogSource replays reads and records postfix_up before each one; when out of reads it cancels ctx.
type scriptedLogSource struct {
	logsource.LogSourceDefaults
	exporter *PostfixExporter
	cancel   context.CancelFunc
	reads    []scriptedRead
	upSeen   []float64
	calls    int
}

func (s *scriptedLogSource) Path() string { return "scripted://test" }

func (s *scriptedLogSource) Read(ctx context.Context) (string, error) {
	var m io_prometheus_client.Metric
	_ = s.exporter.postfixUp.WithLabelValues(s.Path()).Write(&m)
	s.upSeen = append(s.upSeen, m.GetGauge().GetValue())
	s.calls++
	if s.calls <= len(s.reads) {
		r := s.reads[s.calls-1]
		return r.line, r.err
	}
	if s.cancel != nil {
		s.cancel()
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func runCollection(t *testing.T, ctx context.Context, src *scriptedLogSource) {
	t.Helper()
	e := NewPostfixExporter(nil, src, false)
	src.exporter = e
	done := make(chan struct{})
	go func() {
		e.StartMetricCollection(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StartMetricCollection did not return after ctx was cancelled")
	}
}

func TestStartMetricCollectionRetriesAfterReadError(t *testing.T) {
	defer func(lo, hi time.Duration) { logRetryMinDelay, logRetryMaxDelay = lo, hi }(logRetryMinDelay, logRetryMaxDelay)
	logRetryMinDelay, logRetryMaxDelay = time.Millisecond, 2*time.Millisecond

	src := &scriptedLogSource{reads: []scriptedRead{
		{line: "", err: errors.New("connection refused")},
		{line: "Oct 11 12:00:00 relay postfix/qmgr[1]: ABC: removed"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	src.cancel = cancel
	runCollection(t, ctx, src)

	// Read after the error sees 0; read after the line sees 1.
	assert.Equal(t, []float64{0, 0, 1}, src.upSeen)
}

func TestStartMetricCollectionStopsOnCancelledContext(t *testing.T) {
	src := &scriptedLogSource{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runCollection(t, ctx, src)

	assert.Equal(t, 1, src.calls)
}
