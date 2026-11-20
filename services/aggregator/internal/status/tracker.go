package status

import (
	"sync"
	"time"

	"bnpb/aggregator/internal/domain"
)

// Tracker maintains upstream provider operational health and degraded timestamps.
type Tracker struct {
	mu                sync.RWMutex
	bmkgHealthy       bool
	bmkgStaleSince    *time.Time
	pvmbgHealthy      bool
	pvmbgStaleSince   *time.Time
	coastalHealthy    bool
	coastalStaleSince *time.Time
}

// NewTracker creates a new Tracker initialized with all upstreams healthy.
func NewTracker() *Tracker {
	return &Tracker{
		bmkgHealthy:    true,
		pvmbgHealthy:   true,
		coastalHealthy: true,
	}
}

// SetBMKGStatus updates the operational health of the BMKG upstream poller.
func (t *Tracker) SetBMKGStatus(healthy bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if healthy {
		t.bmkgHealthy = true
		t.bmkgStaleSince = nil
	} else {
		if t.bmkgHealthy {
			now := time.Now().UTC()
			t.bmkgStaleSince = &now
		}
		t.bmkgHealthy = false
	}
}

// SetPVMBGStatus updates the operational health of the PVMBG upstream poller.
func (t *Tracker) SetPVMBGStatus(healthy bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if healthy {
		t.pvmbgHealthy = true
		t.pvmbgStaleSince = nil
	} else {
		if t.pvmbgHealthy {
			now := time.Now().UTC()
			t.pvmbgStaleSince = &now
		}
		t.pvmbgHealthy = false
	}
}

// SetCoastalStatus updates the operational health of the Coastal Sensor upstream poller.
func (t *Tracker) SetCoastalStatus(healthy bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if healthy {
		t.coastalHealthy = true
		t.coastalStaleSince = nil
	} else {
		if t.coastalHealthy {
			now := time.Now().UTC()
			t.coastalStaleSince = &now
		}
		t.coastalHealthy = false
	}
}

// GetDegradedStatus returns circuit status and oldest stale timestamp based on requested hazard type.
func (t *Tracker) GetDegradedStatus(hazardType *domain.HazardType) (bool, *time.Time) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if hazardType != nil {
		switch *hazardType {
		case domain.HazardTypeSeismic:
			return !t.bmkgHealthy, t.bmkgStaleSince
		case domain.HazardTypeVolcanic:
			return !t.pvmbgHealthy, t.pvmbgStaleSince
		case domain.HazardTypeCoastal:
			return !t.coastalHealthy, t.coastalStaleSince
		case domain.HazardTypeWildfire:
			return false, nil
		}
	}

	// For global/unfiltered queries, degraded if any upstream is down
	if !t.pvmbgHealthy || !t.bmkgHealthy || !t.coastalHealthy {
		var oldest *time.Time
		for _, candidate := range []*time.Time{t.pvmbgStaleSince, t.bmkgStaleSince, t.coastalStaleSince} {
			if candidate != nil && (oldest == nil || candidate.Before(*oldest)) {
				oldest = candidate
			}
		}
		return true, oldest
	}

	return false, nil
}
