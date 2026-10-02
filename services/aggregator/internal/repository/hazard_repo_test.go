package repository

import (
	"testing"

	"bnpb/aggregator/internal/domain"
)

func TestFormatOutboxEventID(t *testing.T) {
	// Standard short ID preserves format
	shortID := formatOutboxEventID(domain.SourceBMKG, "20261002-padang-001")
	expectedShort := "outbox-bmkg-20261002-padang-001"
	if shortID != expectedShort {
		t.Errorf("expected %q, got %q", expectedShort, shortID)
	}

	// Long ID is bounded to <= 64 chars
	longSourceRef := "very-long-unique-station-monitoring-code-from-bmkg-seismic-network-alpha-west-java-indonesia-padang-subduction-9999"
	longID := formatOutboxEventID(domain.SourceBMKG, longSourceRef)
	if len(longID) > 64 {
		t.Errorf("expected length <= 64, got %d (%s)", len(longID), longID)
	}

	// Deterministic: same input produces same output
	longIDRepeat := formatOutboxEventID(domain.SourceBMKG, longSourceRef)
	if longID != longIDRepeat {
		t.Errorf("expected deterministic ID, got %s vs %s", longID, longIDRepeat)
	}

	// Collision-resistant: different long input produces different output
	longSourceRef2 := "very-long-unique-station-monitoring-code-from-bmkg-seismic-network-alpha-west-java-indonesia-padang-subduction-9998"
	longID2 := formatOutboxEventID(domain.SourceBMKG, longSourceRef2)
	if longID == longID2 {
		t.Errorf("collision detected for distinct long IDs: %s", longID)
	}
}
