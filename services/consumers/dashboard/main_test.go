package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

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
		"attributes":{"magnitude":6.2,"confidence_level":0.95},
		"occurred_at":"2026-09-05T15:30:00Z",
		"ingested_at":"2026-09-05T15:35:00Z"
	}`)

	event, err := decodeEvent(payload)
	if err != nil {
		t.Fatalf("decodeEvent() error = %v", err)
	}
	if event.HazardID != "haz-001" || event.HazardType != "SEISMIC" {
		t.Fatalf("decodeEvent() returned unexpected event: %+v", event)
	}
	if event.Attributes["magnitude"] != 6.2 {
		t.Fatalf("magnitude = %v, want 6.2", event.Attributes["magnitude"])
	}
	if event.OccurredAt != time.Date(2026, 9, 5, 15, 30, 0, 0, time.UTC) {
		t.Fatalf("occurred_at = %v", event.OccurredAt)
	}
}

func TestDecodeEventRejectsMissingIdentity(t *testing.T) {
	_, err := decodeEvent([]byte(`{"hazard_type":"SEISMIC","severity":"SIAGA"}`))
	if err == nil {
		t.Fatal("decodeEvent() expected an error for missing hazard_id")
	}
}

func TestDashboardCountsNewEvents(t *testing.T) {
	dashboard := NewDashboard()

	first := dashboard.Process(HazardEvent{
		HazardID:   "haz-001",
		HazardType: "SEISMIC",
		Severity:   "SIAGA",
	})
	second := dashboard.Process(HazardEvent{
		HazardID:   "haz-002",
		HazardType: "SEISMIC",
		Severity:   "SIAGA",
	})

	if first.Duplicate || first.TypeCount != 1 || first.SeverityCount != 1 {
		t.Fatalf("first result = %+v", first)
	}
	if second.Duplicate || second.TypeCount != 2 || second.SeverityCount != 2 {
		t.Fatalf("second result = %+v", second)
	}
}

func TestDashboardSkipsDuplicateHazard(t *testing.T) {
	dashboard := NewDashboard()
	event := HazardEvent{
		HazardID:   "haz-001",
		HazardType: "VOLCANIC",
		Severity:   "AWAS",
	}

	dashboard.Process(event)
	duplicate := dashboard.Process(event)

	if !duplicate.Duplicate {
		t.Fatal("second processing should be marked as duplicate")
	}

	next := dashboard.Process(HazardEvent{
		HazardID:   "haz-002",
		HazardType: "VOLCANIC",
		Severity:   "AWAS",
	})
	if next.TypeCount != 2 || next.SeverityCount != 2 {
		t.Fatalf("duplicate changed counters, next result = %+v", next)
	}
}

func TestMessageHeader(t *testing.T) {
	message := kafka.Message{Headers: []kafka.Header{
		{Key: "X-Correlation-ID", Value: []byte("trace-dashboard")},
	}}

	if got := messageHeader(message, correlationHeader); got != "trace-dashboard" {
		t.Fatalf("messageHeader() = %q, want trace-dashboard", got)
	}
}

func TestConsumeSkipsAndCommitsInvalidMessage(t *testing.T) {
	validEvent := []byte(`{"hazard_id":"haz-valid","hazard_type":"SEISMIC","severity":"SIAGA"}`)
	reader := &scriptedReader{messages: []kafka.Message{
		{Value: []byte(`{"hazard_id":`), Partition: 0, Offset: 10},
		{Value: validEvent, Partition: 0, Offset: 11},
	}}
	dashboard := NewDashboard()

	err := consume(context.Background(), reader, dashboard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("consume() error = %v, want context.Canceled", err)
	}
	if len(reader.committed) != 2 {
		t.Fatalf("committed messages = %d, want 2", len(reader.committed))
	}
	if result := dashboard.Process(HazardEvent{HazardID: "haz-valid", HazardType: "SEISMIC", Severity: "SIAGA"}); !result.Duplicate {
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
