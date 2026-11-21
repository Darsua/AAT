package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestNotifierAlertRules(t *testing.T) {
	tests := []struct {
		name  string
		event HazardEvent
		alert bool
	}{
		{
			name: "major earthquake",
			event: HazardEvent{
				HazardID:   "seismic-major",
				HazardType: "SEISMIC",
				Severity:   "SIAGA",
				Attributes: map[string]any{"magnitude": 6.2},
			},
			alert: true,
		},
		{
			name: "minor earthquake",
			event: HazardEvent{
				HazardID:   "seismic-minor",
				HazardType: "SEISMIC",
				Severity:   "WASPADA",
				Attributes: map[string]any{"magnitude": 5.9},
			},
			alert: false,
		},
		{
			name: "tsunami siaga",
			event: HazardEvent{
				HazardID:   "tsunami-siaga",
				HazardType: "SEISMIC",
				Severity:   "SIAGA",
				Attributes: map[string]any{
					"tsunami_warning": map[string]any{"threat_level": "Siaga"},
				},
			},
			alert: true,
		},
		{
			name: "volcano awas",
			event: HazardEvent{
				HazardID:   "volcano-awas",
				HazardType: "VOLCANIC",
				Severity:   "AWAS",
			},
			alert: true,
		},
		{
			name: "volcano siaga",
			event: HazardEvent{
				HazardID:   "volcano-siaga",
				HazardType: "VOLCANIC",
				Severity:   "SIAGA",
			},
			alert: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := NewNotifier().Process(test.event)
			if result.Alert != test.alert {
				t.Fatalf("alert = %v, want %v; reason=%q", result.Alert, test.alert, result.Reason)
			}
		})
	}
}

func TestNotifierCombinesAlertReasons(t *testing.T) {
	result := NewNotifier().Process(HazardEvent{
		HazardID:   "seismic-tsunami",
		HazardType: "SEISMIC",
		Severity:   "AWAS",
		Attributes: map[string]any{
			"magnitude":       7.1,
			"tsunami_warning": map[string]any{"threat_level": "Awas"},
		},
	})

	if !result.Alert {
		t.Fatal("expected an alert")
	}
	if result.Reason != "earthquake magnitude 7.1; tsunami warning AWAS" {
		t.Fatalf("reason = %q", result.Reason)
	}
}

func TestNotifierSkipsDuplicateHazard(t *testing.T) {
	notifier := NewNotifier()
	event := HazardEvent{
		HazardID:   "haz-001",
		HazardType: "VOLCANIC",
		Severity:   "AWAS",
	}

	first := notifier.Process(event)
	second := notifier.Process(event)

	if !first.Alert || first.Duplicate {
		t.Fatalf("first result = %+v", first)
	}
	if !second.Duplicate || second.Alert {
		t.Fatalf("second result = %+v", second)
	}
}

func TestDecodeEvent(t *testing.T) {
	payload := []byte(`{
		"hazard_id":"haz-001",
		"source":"BMKG",
		"source_ref_id":"bmkg-001",
		"hazard_type":"SEISMIC",
		"severity":"SIAGA",
		"area_name":"Bantul",
		"latitude":-7.54,
		"longitude":110.44,
		"attributes":{"magnitude":6.2},
		"occurred_at":"2026-09-05T15:30:00Z",
		"ingested_at":"2026-09-05T15:35:00Z"
	}`)

	event, err := decodeEvent(payload)
	if err != nil {
		t.Fatalf("decodeEvent() error = %v", err)
	}
	if event.HazardID != "haz-001" || event.Attributes["magnitude"] != 6.2 {
		t.Fatalf("decodeEvent() returned unexpected event: %+v", event)
	}
}

func TestMessageHeader(t *testing.T) {
	message := kafka.Message{Headers: []kafka.Header{
		{Key: "x-correlation-id", Value: []byte("trace-notifier")},
	}}

	if got := messageHeader(message, correlationHeader); got != "trace-notifier" {
		t.Fatalf("messageHeader() = %q, want trace-notifier", got)
	}
}

func TestConsumeSkipsAndCommitsInvalidMessage(t *testing.T) {
	validEvent := []byte(`{"hazard_id":"haz-valid","hazard_type":"VOLCANIC","severity":"AWAS"}`)
	reader := &scriptedReader{messages: []kafka.Message{
		{Value: []byte(`{"hazard_id":`), Partition: 1, Offset: 20},
		{Value: validEvent, Partition: 1, Offset: 21},
	}}
	notifier := NewNotifier()

	err := consume(context.Background(), reader, notifier)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("consume() error = %v, want context.Canceled", err)
	}
	if len(reader.committed) != 2 {
		t.Fatalf("committed messages = %d, want 2", len(reader.committed))
	}
	if result := notifier.Process(HazardEvent{HazardID: "haz-valid", HazardType: "VOLCANIC", Severity: "AWAS"}); !result.Duplicate {
		t.Fatal("valid message after poison message was not processed")
	}
}

func TestNotifierUpdateAwareDedupAndCoastalCriticalDelivery(t *testing.T) {
	var received []NotificationDelivery
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var d NotificationDelivery
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received = append(received, d)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	receiptPath := filepath.Join(t.TempDir(), "receipts.jsonl")
	n := NewNotifierWithOptions(receiptPath, receiver.URL)

	// 1. Initial non-critical coastal event -> no alert.
	ev1 := HazardEvent{
		HazardID:   "haz-pantai-obs-1",
		Source:     "SENSOR_PANTAI",
		HazardType: "COASTAL",
		Severity:   "NORMAL",
		AreaName:   "Padang, Sumatera Barat",
		OccurredAt: time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC),
		Attributes: map[string]any{"critical": false, "wave_height_m": 0.9},
	}
	res1 := n.Process(ev1)
	if res1.Duplicate || res1.Alert {
		t.Fatalf("expected non-duplicate non-alert, got %+v", res1)
	}

	// 2. Escalated update with same hazard_id and critical=true -> NOT suppressed, triggers alert.
	ev2 := ev1
	ev2.Severity = "AWAS"
	ev2.Attributes = map[string]any{"critical": true, "wave_height_m": 4.5}
	res2 := n.Process(ev2)
	if res2.Duplicate || !res2.Alert {
		t.Fatalf("expected updated critical coastal event to alert, got %+v", res2)
	}
	if _, err := n.Deliver(context.Background(), ev2, res2.Reason, "corr-coastal-1"); err != nil {
		t.Fatalf("Deliver failed: %v", err)
	}
	if len(received) != 1 || received[0].HazardID != "haz-pantai-obs-1" {
		t.Fatalf("expected 1 webhook delivery, got %+v", received)
	}

	// 3. Reload from durable receipt log on restart -> identical ev2 is now suppressed as duplicate.
	restarted := NewNotifierWithOptions(receiptPath, receiver.URL)
	if len(restarted.Deliveries()) != 1 {
		t.Fatalf("expected 1 persisted receipt after restart, got %d", len(restarted.Deliveries()))
	}
	res3 := restarted.Process(ev2)
	if !res3.Duplicate {
		t.Fatalf("expected identical event after restart to be duplicate, got %+v", res3)
	}
}

type scriptedReader struct {
	messages  []kafka.Message
	committed []kafka.Message
}

func (r *scriptedReader) FetchMessage(context.Context) (kafka.Message, error) {
	if len(r.messages) == 0 {
		return kafka.Message{}, context.Canceled
	}
	message := r.messages[0]
	r.messages = r.messages[1:]
	return message, nil
}

func (r *scriptedReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	r.committed = append(r.committed, messages...)
	return nil
}

func TestConsumeDoesNotMarkDuplicateOnDeliveryFailure(t *testing.T) {
	var failWebhook atomic.Bool
	failWebhook.Store(true)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failWebhook.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	ev := []byte(`{"hazard_id":"haz-retry-1","hazard_type":"VOLCANIC","severity":"AWAS"}`)
	n := NewNotifierWithOptions(filepath.Join(t.TempDir(), "r.jsonl"), receiver.URL)

	// First attempt fails webhook delivery -> error returned, offset NOT committed, NOT marked processed.
	r1 := &scriptedReader{messages: []kafka.Message{{Value: ev, Partition: 0, Offset: 1}}}
	if err := consume(context.Background(), r1, n); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected delivery error on first attempt, got %v", err)
	}
	if len(r1.committed) != 0 {
		t.Fatalf("expected 0 commits on failed delivery, got %d", len(r1.committed))
	}

	// Redelivery succeeds once receiver recovers and is NOT suppressed as duplicate.
	failWebhook.Store(false)
	r2 := &scriptedReader{messages: []kafka.Message{{Value: ev, Partition: 0, Offset: 1}}}
	if err := consume(context.Background(), r2, n); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected clean second pass ending with context.Canceled, got %v", err)
	}
	if len(r2.committed) != 1 || len(n.Deliveries()) != 1 {
		t.Fatalf("expected 1 committed delivery on retry, committed=%d deliveries=%d", len(r2.committed), len(n.Deliveries()))
	}
}
