package satellite

import (
	"fmt"
	"sort"
	"time"
)

// DetectionState is the serializable form of a Detection.
type DetectionState struct {
	Satellite  string  `json:"satellite"`
	AcqDate    string  `json:"acq_date"`
	AcqTime    string  `json:"acq_time"`
	Latitude   string  `json:"latitude"`
	Longitude  string  `json:"longitude"`
	Confidence string  `json:"confidence"`
	FRP        float64 `json:"frp"`
}

// ComponentState is one connected component; ID is empty for components that have
// never qualified as a fire.
type ComponentState struct {
	ID   string   `json:"id,omitempty"`
	Keys []string `json:"keys"`
}

// State is a complete, JSON-serializable engine snapshot. Persisting it together with the
// feed offset of the last applied event gives a coherent replay boundary: restore, then
// resume the stream after that offset.
type State struct {
	Detections   []DetectionState `json:"detections"`
	Components   []ComponentState `json:"components"`
	Late         []DetectionState `json:"late"`
	Low          []DetectionState `json:"low,omitempty"`
	UsedIDs      []string         `json:"used_ids"`
	MaxEventTime time.Time        `json:"max_event_time"`
	Stats        Stats            `json:"stats"`
}

func toState(d Detection) DetectionState {
	return DetectionState{d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude, d.Confidence, d.FRP}
}

func (s DetectionState) detection() (Detection, error) {
	return NewDetection(s.Satellite, s.AcqDate, s.AcqTime, s.Latitude, s.Longitude, s.Confidence, s.FRP)
}

// Snapshot captures the engine state deterministically (all slices sorted).
func (e *Engine) Snapshot() State {
	st := State{MaxEventTime: e.maxEventTime, Stats: e.stats}
	for _, k := range sortedKeys(keySet(e.detections)) {
		st.Detections = append(st.Detections, toState(e.detections[k]))
	}
	for _, k := range sortedKeys(keySet(e.late)) {
		st.Late = append(st.Late, toState(e.late[k]))
	}
	for _, k := range sortedKeys(keySet(e.low)) {
		st.Low = append(st.Low, toState(e.low[k]))
	}
	seen := make(map[*component]struct{})
	for _, c := range e.compOf {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		st.Components = append(st.Components, ComponentState{ID: c.id, Keys: sortedKeys(c.keys)})
	}
	sort.Slice(st.Components, func(i, j int) bool { return st.Components[i].Keys[0] < st.Components[j].Keys[0] })
	st.UsedIDs = sortedKeys(e.usedIDs)
	return st
}

// Restore rebuilds an engine from a snapshot, validating that components cover every
// detection exactly once.
func Restore(cfg Config, st State) (*Engine, error) {
	e := NewEngine(cfg)
	for _, ds := range st.Detections {
		d, err := ds.detection()
		if err != nil {
			return nil, fmt.Errorf("restore detection: %w", err)
		}
		e.detections[d.Key()] = d
		e.index(d)
	}
	for _, ds := range st.Late {
		d, err := ds.detection()
		if err != nil {
			return nil, fmt.Errorf("restore late detection: %w", err)
		}
		e.late[d.Key()] = d
	}
	for _, ds := range st.Low {
		d, err := ds.detection()
		if err != nil {
			return nil, fmt.Errorf("restore low-confidence detection: %w", err)
		}
		e.low[d.Key()] = d
	}
	for _, cs := range st.Components {
		c := &component{id: cs.ID, keys: make(map[string]struct{}, len(cs.Keys))}
		for _, k := range cs.Keys {
			if _, ok := e.detections[k]; !ok {
				return nil, fmt.Errorf("component references unknown detection %q", k)
			}
			if _, dup := e.compOf[k]; dup {
				return nil, fmt.Errorf("detection %q appears in more than one component", k)
			}
			c.keys[k] = struct{}{}
			e.compOf[k] = c
		}
	}
	if len(e.compOf) != len(e.detections) {
		return nil, fmt.Errorf("components cover %d of %d detections", len(e.compOf), len(e.detections))
	}
	for _, id := range st.UsedIDs {
		e.usedIDs[id] = struct{}{}
	}
	e.maxEventTime = st.MaxEventTime
	e.stats = st.Stats
	return e, nil
}

func keySet[V any](m map[string]V) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}
