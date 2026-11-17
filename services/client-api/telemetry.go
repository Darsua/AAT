package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	traceparentHeader = "traceparent"
	spanKindInternal  = 1
	spanKindServer    = 2
	spanKindClient    = 3
	spanKindProducer  = 4
	spanKindConsumer  = 5
)

var defaultLatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type telemetryContextKey string

const (
	traceContextKey telemetryContextKey = "trace-context"
)

type traceState struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Sampled      bool
}

// SpanAttr is a key-value attribute attached to an exported OTLP span.
type SpanAttr struct {
	Key   string
	Value string
}

// Registry is a lightweight, stdlib-only Prometheus metrics registry.
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]map[string]float64
	gauges     map[string]map[string]float64
	histograms map[string]map[string]*histogramData
	helps      map[string]string
	types      map[string]string
}

type histogramData struct {
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

func newRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]map[string]float64),
		gauges:     make(map[string]map[string]float64),
		histograms: make(map[string]map[string]*histogramData),
		helps:      make(map[string]string),
		types:      make(map[string]string),
	}
}

func (r *Registry) RegisterMeta(name, metricType, help string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types[name] = metricType
	r.helps[name] = help
}

func (r *Registry) IncCounter(name string, labels map[string]string, delta float64) {
	key := formatLabels(labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.types[name]; !ok {
		r.types[name] = "counter"
	}
	series, ok := r.counters[name]
	if !ok {
		series = make(map[string]float64)
		r.counters[name] = series
	}
	series[key] += delta
}

func (r *Registry) SetGauge(name string, labels map[string]string, value float64) {
	key := formatLabels(labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.types[name]; !ok {
		r.types[name] = "gauge"
	}
	series, ok := r.gauges[name]
	if !ok {
		series = make(map[string]float64)
		r.gauges[name] = series
	}
	series[key] = value
}

func (r *Registry) ObserveHistogram(name string, labels map[string]string, value float64) {
	key := formatLabels(labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.types[name]; !ok {
		r.types[name] = "histogram"
	}
	series, ok := r.histograms[name]
	if !ok {
		series = make(map[string]*histogramData)
		r.histograms[name] = series
	}
	h, ok := series[key]
	if !ok {
		h = &histogramData{
			buckets: defaultLatencyBuckets,
			counts:  make([]uint64, len(defaultLatencyBuckets)),
		}
		series[key] = h
	}
	for i, bound := range h.buckets {
		if value <= bound {
			h.counts[i]++
		}
	}
	h.sum += value
	h.count++
}

func (r *Registry) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var buf bytes.Buffer

		r.mu.RLock()
		names := make([]string, 0, len(r.counters)+len(r.gauges)+len(r.histograms))
		seen := make(map[string]struct{})
		for k := range r.counters {
			names = append(names, k)
			seen[k] = struct{}{}
		}
		for k := range r.gauges {
			if _, ok := seen[k]; !ok {
				names = append(names, k)
				seen[k] = struct{}{}
			}
		}
		for k := range r.histograms {
			if _, ok := seen[k]; !ok {
				names = append(names, k)
			}
		}
		sort.Strings(names)

		for _, name := range names {
			if help := r.helps[name]; help != "" {
				fmt.Fprintf(&buf, "# HELP %s %s\n", name, help)
			}
			if typ := r.types[name]; typ != "" {
				fmt.Fprintf(&buf, "# TYPE %s %s\n", name, typ)
			}
			if series, ok := r.counters[name]; ok {
				for _, k := range sortedKeys(series) {
					fmt.Fprintf(&buf, "%s%s %g\n", name, k, series[k])
				}
			}
			if series, ok := r.gauges[name]; ok {
				for _, k := range sortedKeys(series) {
					fmt.Fprintf(&buf, "%s%s %g\n", name, k, series[k])
				}
			}
			if series, ok := r.histograms[name]; ok {
				hKeys := make([]string, 0, len(series))
				for k := range series {
					hKeys = append(hKeys, k)
				}
				sort.Strings(hKeys)
				for _, k := range hKeys {
					h := series[k]
					for i, bound := range h.buckets {
						leLabel := addLabel(k, "le", strconv.FormatFloat(bound, 'f', -1, 64))
						fmt.Fprintf(&buf, "%s_bucket%s %d\n", name, leLabel, h.counts[i])
					}
					infLabel := addLabel(k, "le", "+Inf")
					fmt.Fprintf(&buf, "%s_bucket%s %d\n", name, infLabel, h.count)
					fmt.Fprintf(&buf, "%s_sum%s %g\n", name, k, h.sum)
					fmt.Fprintf(&buf, "%s_count%s %d\n", name, k, h.count)
				}
			}
		}
		r.mu.RUnlock()

		_, _ = w.Write(buf.Bytes())
	}
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func addLabel(formatted, key, val string) string {
	pair := fmt.Sprintf(`%s="%s"`, key, escapeLabelValue(val))
	if formatted == "" {
		return "{" + pair + "}"
	}
	return formatted[:len(formatted)-1] + "," + pair + "}"
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Tracer handles W3C traceparent propagation and non-blocking OTLP/HTTP JSON span export.
type Tracer struct {
	serviceName string
	endpoint    string
	client      *http.Client
	spans       chan otlpSpanRecord
	logger      *slog.Logger
}

type otlpSpanRecord struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	Kind         int
	StartTime    time.Time
	EndTime      time.Time
	StatusCode   int
	StatusMsg    string
	Attrs        []SpanAttr
}

// Span represents an active trace span.
type Span struct {
	tracer       *Tracer
	traceID      string
	spanID       string
	parentSpanID string
	name         string
	kind         int
	startTime    time.Time
	attrs        []SpanAttr
}

func newTracer(serviceName string, logger *slog.Logger) *Tracer {
	if envSvc := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); envSvc != "" {
		serviceName = envSvc
	}
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")), "/")
	t := &Tracer{
		serviceName: serviceName,
		endpoint:    endpoint,
		client:      &http.Client{Timeout: 2 * time.Second},
		logger:      logger,
	}
	if endpoint != "" {
		t.spans = make(chan otlpSpanRecord, 1024)
		go t.exportLoop()
	}
	return t
}

func (t *Tracer) StartSpan(ctx context.Context, name string, kind int, attrs ...SpanAttr) (context.Context, *Span) {
	parent, ok := ctx.Value(traceContextKey).(traceState)
	traceID := ""
	parentSpanID := ""
	sampled := true
	if ok && parent.TraceID != "" {
		traceID = parent.TraceID
		parentSpanID = parent.SpanID
		sampled = parent.Sampled
	} else {
		traceID = randomHex(16)
	}
	spanID := randomHex(8)
	nextState := traceState{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		Sampled:      sampled,
	}
	ctx = context.WithValue(ctx, traceContextKey, nextState)
	return ctx, &Span{
		tracer:       t,
		traceID:      traceID,
		spanID:       spanID,
		parentSpanID: parentSpanID,
		name:         name,
		kind:         kind,
		startTime:    time.Now(),
		attrs:        attrs,
	}
}

func (s *Span) End(statusCode int, statusMsg string) {
	if s == nil || s.tracer == nil || s.tracer.spans == nil {
		return
	}
	rec := otlpSpanRecord{
		TraceID:      s.traceID,
		SpanID:       s.spanID,
		ParentSpanID: s.parentSpanID,
		Name:         s.name,
		Kind:         s.kind,
		StartTime:    s.startTime,
		EndTime:      time.Now(),
		StatusCode:   statusCode,
		StatusMsg:    statusMsg,
		Attrs:        s.attrs,
	}
	select {
	case s.tracer.spans <- rec:
	default:
		// Drop non-blockingly if exporter queue is full.
	}
}

func (t *Tracer) exportLoop() {
	for rec := range t.spans {
		t.sendOTLP(rec)
	}
}

func (t *Tracer) sendOTLP(rec otlpSpanRecord) {
	attrs := make([]map[string]any, 0, len(rec.Attrs))
	for _, a := range rec.Attrs {
		attrs = append(attrs, map[string]any{
			"key":   a.Key,
			"value": map[string]any{"stringValue": a.Value},
		})
	}
	otlpStatus := 1 // OK
	if rec.StatusCode >= 500 {
		otlpStatus = 2 // ERROR
	}
	spanObj := map[string]any{
		"traceId":           rec.TraceID,
		"spanId":            rec.SpanID,
		"name":              rec.Name,
		"kind":              rec.Kind,
		"startTimeUnixNano": strconv.FormatInt(rec.StartTime.UnixNano(), 10),
		"endTimeUnixNano":   strconv.FormatInt(rec.EndTime.UnixNano(), 10),
		"attributes":        attrs,
		"status": map[string]any{
			"code":    otlpStatus,
			"message": rec.StatusMsg,
		},
	}
	if rec.ParentSpanID != "" {
		spanObj["parentSpanId"] = rec.ParentSpanID
	}

	payload := map[string]any{
		"resourceSpans": []any{
			map[string]any{
				"resource": map[string]any{
					"attributes": []any{
						map[string]any{
							"key":   "service.name",
							"value": map[string]any{"stringValue": t.serviceName},
						},
					},
				},
				"scopeSpans": []any{
					map[string]any{
						"scope": map[string]any{"name": "bnpb.telemetry"},
						"spans": []any{spanObj},
					},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, t.endpoint+"/v1/traces", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func parseTraceparent(header string) (traceID, parentSpanID string, sampled bool, ok bool) {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) != 4 {
		return "", "", false, false
	}
	if len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", false, false
	}
	if !isLowerHex(parts[1]) || !isLowerHex(parts[2]) || parts[1] == strings.Repeat("0", 32) || parts[2] == strings.Repeat("0", 16) {
		return "", "", false, false
	}
	flags, err := strconv.ParseUint(parts[3], 16, 8)
	if err != nil {
		return "", "", false, false
	}
	return parts[1], parts[2], (flags & 0x01) != 0, true
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func formatTraceparent(traceID, spanID string, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", traceID, spanID, flags)
}

func traceFromContext(ctx context.Context) traceState {
	st, _ := ctx.Value(traceContextKey).(traceState)
	return st
}

func contextWithTraceparent(ctx context.Context, header string) context.Context {
	traceID, spanID, sampled, ok := parseTraceparent(header)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey, traceState{
		TraceID: traceID,
		SpanID:  spanID,
		Sampled: sampled,
	})
}

func injectTraceHTTP(ctx context.Context, req *http.Request) {
	if st := traceFromContext(ctx); st.TraceID != "" && st.SpanID != "" {
		req.Header.Set(traceparentHeader, formatTraceparent(st.TraceID, st.SpanID, st.Sampled))
	}
	if corrID := correlationIDFromContext(ctx); corrID != "" {
		req.Header.Set(correlationHead, corrID)
	}
}

func randomHex(numBytes int) string {
	b := make([]byte, numBytes)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%0*x", numBytes*2, time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func newJSONLogger(serviceName string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With(slog.String("service", serviceName))
}

func observabilityMiddleware(serviceName string, reg *Registry, tracer *Tracer, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if tp := r.Header.Get(traceparentHeader); tp != "" {
			ctx = contextWithTraceparent(ctx, tp)
		}
		corrID := correlationIDFromContext(ctx)
		route := r.URL.Path
		ctx, span := tracer.StartSpan(ctx, r.Method+" "+route, spanKindServer,
			SpanAttr{Key: "http.method", Value: r.Method},
			SpanAttr{Key: "http.target", Value: route},
			SpanAttr{Key: "correlation_id", Value: corrID},
		)
		if st := traceFromContext(ctx); st.TraceID != "" {
			w.Header().Set(traceparentHeader, formatTraceparent(st.TraceID, st.SpanID, st.Sampled))
		}

		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r.WithContext(ctx))

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		dur := time.Since(started)
		span.End(status, http.StatusText(status))

		if r.URL.Path != "/metrics" {
			labels := map[string]string{
				"service": serviceName,
				"method":  r.Method,
				"route":   route,
				"status":  strconv.Itoa(status),
			}
			reg.IncCounter("http_requests_total", labels, 1)
			reg.ObserveHistogram("http_request_duration_seconds", map[string]string{
				"service": serviceName,
				"method":  r.Method,
				"route":   route,
			}, dur.Seconds())
		}

		st := traceFromContext(ctx)
		logger.Info("http request",
			slog.String("correlation_id", corrID),
			slog.String("trace_id", st.TraceID),
			slog.String("span_id", st.SpanID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.Int64("duration_ms", dur.Milliseconds()),
		)
	})
}
