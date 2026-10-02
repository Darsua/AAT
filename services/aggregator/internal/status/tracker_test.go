package status

import (
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
)

func TestTracker_InitialState(t *testing.T) {
	tracker := NewTracker()

	degraded, staleSince := tracker.GetDegradedStatus(nil)
	if degraded || staleSince != nil {
		t.Errorf("expected initially healthy, got degraded=%v, staleSince=%v", degraded, staleSince)
	}

	seisType := domain.HazardTypeSeismic
	degraded, staleSince = tracker.GetDegradedStatus(&seisType)
	if degraded || staleSince != nil {
		t.Errorf("expected seismic initially healthy, got degraded=%v, staleSince=%v", degraded, staleSince)
	}
}

func TestTracker_PVMBGOutageAndIsolation(t *testing.T) {
	tracker := NewTracker()

	// Simulate PVMBG outage
	tracker.SetPVMBGStatus(false)

	// 1. Volcanic query must report degraded
	volcType := domain.HazardTypeVolcanic
	degraded, staleSince := tracker.GetDegradedStatus(&volcType)
	if !degraded || staleSince == nil {
		t.Fatalf("expected volcanic query to report degraded with staleSince")
	}

	// 2. Global query must report degraded
	degradedGlobal, staleGlobal := tracker.GetDegradedStatus(nil)
	if !degradedGlobal || staleGlobal == nil {
		t.Fatalf("expected global query to report degraded with staleSince")
	}

	// 3. Fault Isolation: Seismic query must remain NOT degraded!
	seisType := domain.HazardTypeSeismic
	degradedSeis, staleSeis := tracker.GetDegradedStatus(&seisType)
	if degradedSeis || staleSeis != nil {
		t.Fatalf("fault isolation failed: seismic should remain healthy during PVMBG outage, got degraded=%v", degradedSeis)
	}

	// 4. Recover PVMBG
	time.Sleep(10 * time.Millisecond)
	tracker.SetPVMBGStatus(true)

	degraded, staleSince = tracker.GetDegradedStatus(&volcType)
	if degraded || staleSince != nil {
		t.Errorf("expected volcanic healthy after recovery, got degraded=%v, staleSince=%v", degraded, staleSince)
	}
}
