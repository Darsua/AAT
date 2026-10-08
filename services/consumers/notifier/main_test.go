package main

import (
	"context"
	"errors"
	"testing"

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
