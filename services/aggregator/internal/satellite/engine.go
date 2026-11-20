package satellite

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

// Connectivity and lateness rules from issue #28.
const (
	DefaultMaxDistanceKm = 1.0
	DefaultMaxGap        = 360 * time.Minute
	DefaultLateness      = 6 * time.Hour

	// stripDegrees is the latitude band used by the spatial index. One band (~1.11 km)
	// is wider than the 1 km connection radius, so neighbors are always in adjacent bands.
	stripDegrees = 0.01
)

// Config tunes the engine. Zero values select the issue #28 defaults.
type Config struct {
	MaxDistanceKm float64
	MaxGap        time.Duration
	// Lateness is how far behind the newest acquisition time a new detection may be and
	// still be clustered. Older detections are recorded as late, not silently dropped.
	Lateness time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxDistanceKm <= 0 {
		c.MaxDistanceKm = DefaultMaxDistanceKm
	}
	if c.MaxGap <= 0 {
		c.MaxGap = DefaultMaxGap
	}
	if c.Lateness <= 0 {
		c.Lateness = DefaultLateness
	}
	return c
}

// Disposition records what the engine did with one feed event.
type Disposition string

const (
	DispositionAccepted      Disposition = "accepted"
	DispositionDuplicate     Disposition = "duplicate"
	DispositionUpdated       Disposition = "updated"
	DispositionRetracted     Disposition = "retracted"
	DispositionLowConfidence Disposition = "low_confidence"
	DispositionLate          Disposition = "late_beyond_watermark"
	DispositionUnknown       Disposition = "unknown_detection"
	DispositionHeartbeat     Disposition = "heartbeat"
)

// ChangeKind says whether a fire hazard must be written or retired.
type ChangeKind string

const (
	ChangeUpsert ChangeKind = "upsert"
	ChangeRetire ChangeKind = "retire"
)

// Retirement reasons for ChangeRetire.
const (
	RetireMerged         = "merged"
	RetireBelowThreshold = "below_threshold"
)

// Change is one fire lifecycle transition the caller must persist.
type Change struct {
	Kind     ChangeKind
	Fire     Fire
	Reason   string // set for ChangeRetire
	MergedTo string // surviving fire ID when Reason is RetireMerged
}

// Stats counts dispositions and the current singleton detections.
type Stats struct {
	Accepted      int `json:"accepted"`
	Duplicates    int `json:"duplicates"`
	Updated       int `json:"updated"`
	Retracted     int `json:"retracted"`
	LowConfidence int `json:"low_confidence"`
	Late          int `json:"late_beyond_watermark"`
	Unknown       int `json:"unknown_detection"`
	Singletons    int `json:"singletons"`
	ClosedFires   int `json:"closed_fires,omitempty"`
	Evicted       int `json:"evicted_detections,omitempty"`
}

type component struct {
	id   string // public fire ID; empty while the component has never qualified as a fire
	keys map[string]struct{}
}

// Engine maintains connected wildfire components incrementally. Each call applies exactly
// one feed event, so the state after any prefix of the stream is a pure function of that
// prefix and replaying from a checkpoint reproduces the uninterrupted result.
// Engine is not safe for concurrent use.
type Engine struct {
	cfg          Config
	detections   map[string]Detection
	compOf       map[string]*component
	strips       map[int64]map[string]struct{}
	usedIDs      map[string]struct{}
	late         map[string]Detection
	low          map[string]Detection // low-confidence detections, kept so updates can patch them
	maxEventTime time.Time
	stats        Stats
}

// NewEngine returns an empty engine.
func NewEngine(cfg Config) *Engine {
	return &Engine{
		cfg:        cfg.withDefaults(),
		detections: make(map[string]Detection),
		compOf:     make(map[string]*component),
		strips:     make(map[int64]map[string]struct{}),
		usedIDs:    make(map[string]struct{}),
		late:       make(map[string]Detection),
		low:        make(map[string]Detection),
	}
}

// Lookup returns the stored detection for key, whether clustered, late, or low-confidence.
func (e *Engine) Lookup(key string) (Detection, bool) {
	if d, ok := e.detections[key]; ok {
		return d, true
	}
	if d, ok := e.late[key]; ok {
		return d, true
	}
	d, ok := e.low[key]
	return d, ok
}

// Watermark is the oldest acquisition time a new detection may have and still be clustered.
func (e *Engine) Watermark() time.Time {
	if e.maxEventTime.IsZero() {
		return time.Time{}
	}
	return e.maxEventTime.Add(-e.cfg.Lateness)
}

// MinRetention is the shortest safe Evict horizon: a new detection is accepted only
// within Lateness of the newest acquisition and links only within MaxGap, so a
// component whose newest detection is older than both can never grow again.
func (e *Engine) MinRetention() time.Duration { return e.cfg.Lateness + e.cfg.MaxGap }

// Evict closes components whose newest detection is more than horizon behind the newest
// acquisition time, dropping their detections from memory and from later snapshots.
// Their fires are final and stay persisted as they are; singletons stay counted. A
// horizon below MinRetention is raised to it. Updates or retracts that later name an
// evicted detection count as unknown, and a re-delivered one counts as late. It
// returns the IDs of the fires it closed and the number of detections dropped.
func (e *Engine) Evict(horizon time.Duration) (closed []string, dropped int) {
	if e.maxEventTime.IsZero() {
		return nil, 0
	}
	if min := e.MinRetention(); horizon < min {
		horizon = min
	}
	cutoff := e.maxEventTime.Add(-horizon)
	newest := make(map[*component]time.Time)
	for k, c := range e.compOf {
		if t := e.detections[k].acquired; t.After(newest[c]) {
			newest[c] = t
		}
	}
	for c, t := range newest {
		if !t.Before(cutoff) {
			continue
		}
		if c.id != "" && len(c.keys) >= minFireDetections {
			e.stats.ClosedFires++
			closed = append(closed, c.id)
		}
		for k := range c.keys {
			e.unindex(e.detections[k])
			delete(e.detections, k)
			delete(e.compOf, k)
			dropped++
		}
	}
	for k, d := range e.low {
		if d.acquired.Before(cutoff) {
			delete(e.low, k)
			dropped++
		}
	}
	for k, d := range e.late {
		if d.acquired.Before(cutoff) {
			delete(e.late, k)
			dropped++
		}
	}
	e.stats.Evicted += dropped
	sort.Strings(closed)
	return closed, dropped
}

// Stats returns disposition counters.
func (e *Engine) Stats() Stats { return e.stats }

// Late returns detections recorded beyond the watermark, sorted by key.
func (e *Engine) Late() []Detection {
	out := make([]Detection, 0, len(e.late))
	for _, d := range e.late {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

// Add applies a detection event.
func (e *Engine) Add(d Detection) (Disposition, []Change) {
	key := d.Key()
	if _, ok := e.detections[key]; ok {
		e.stats.Duplicates++
		return DispositionDuplicate, nil
	}
	if _, ok := e.late[key]; ok {
		e.stats.Duplicates++
		return DispositionDuplicate, nil
	}
	if _, ok := e.low[key]; ok {
		e.stats.Duplicates++
		return DispositionDuplicate, nil
	}
	if d.LowConfidence() {
		e.low[key] = d
		e.stats.LowConfidence++
		return DispositionLowConfidence, nil
	}
	if wm := e.Watermark(); !wm.IsZero() && d.acquired.Before(wm) {
		e.late[key] = d
		e.stats.Late++
		return DispositionLate, nil
	}
	if d.acquired.After(e.maxEventTime) {
		e.maxEventTime = d.acquired
	}
	e.stats.Accepted++
	return DispositionAccepted, e.insert(d)
}

// Update applies an update event. Identity fields cannot change, so connectivity is
// unchanged; an update that drops to low confidence removes the detection.
func (e *Engine) Update(d Detection) (Disposition, []Change) {
	key := d.Key()
	if _, ok := e.late[key]; ok {
		e.late[key] = d
		e.stats.Updated++
		return DispositionUpdated, nil
	}
	if _, ok := e.low[key]; ok {
		e.stats.Updated++
		if d.LowConfidence() {
			e.low[key] = d
			return DispositionUpdated, nil
		}
		// Raised above low confidence: it now takes part in clustering.
		delete(e.low, key)
		disp, changes := e.Add(d)
		if disp == DispositionAccepted {
			disp = DispositionUpdated
		}
		return disp, changes
	}
	if _, ok := e.detections[key]; !ok {
		return e.Add(d)
	}
	if d.LowConfidence() {
		e.stats.Updated++
		changes := e.remove(key)
		e.low[key] = d
		return DispositionUpdated, changes
	}
	e.detections[key] = d
	e.stats.Updated++
	comp := e.compOf[key]
	if len(comp.keys) < minFireDetections {
		return DispositionUpdated, nil
	}
	return DispositionUpdated, []Change{{Kind: ChangeUpsert, Fire: e.buildFire(comp)}}
}

// Retract removes a detection by key.
func (e *Engine) Retract(key string) (Disposition, []Change) {
	if _, ok := e.late[key]; ok {
		delete(e.late, key)
		e.stats.Retracted++
		return DispositionRetracted, nil
	}
	if _, ok := e.low[key]; ok {
		delete(e.low, key)
		e.stats.Retracted++
		return DispositionRetracted, nil
	}
	if _, ok := e.detections[key]; !ok {
		e.stats.Unknown++
		return DispositionUnknown, nil
	}
	e.stats.Retracted++
	return DispositionRetracted, e.remove(key)
}

// Fires returns all current fires sorted by ID.
func (e *Engine) Fires() []Fire {
	seen := make(map[*component]struct{})
	var fires []Fire
	for _, comp := range e.compOf {
		if _, ok := seen[comp]; ok || len(comp.keys) < minFireDetections {
			continue
		}
		seen[comp] = struct{}{}
		fires = append(fires, e.buildFire(comp))
	}
	sort.Slice(fires, func(i, j int) bool { return fires[i].ID < fires[j].ID })
	return fires
}

func (e *Engine) insert(d Detection) []Change {
	key := d.Key()
	neighbors := e.neighborComponents(d)
	e.detections[key] = d
	e.index(d)

	if len(neighbors) == 0 {
		e.compOf[key] = &component{keys: map[string]struct{}{key: {}}}
		e.stats.Singletons++
		return nil
	}

	// Survivor: the existing fire with the most detections; ties go to the smallest ID.
	var survivor *component
	for _, c := range neighbors {
		if c.id == "" {
			continue
		}
		if survivor == nil || len(c.keys) > len(survivor.keys) || (len(c.keys) == len(survivor.keys) && c.id < survivor.id) {
			survivor = c
		}
	}
	merged := &component{keys: map[string]struct{}{key: {}}}
	var changes []Change
	for _, c := range neighbors {
		if len(c.keys) == 1 {
			e.stats.Singletons--
		}
		for k := range c.keys {
			merged.keys[k] = struct{}{}
		}
	}
	for _, k := range sortedKeys(merged.keys) {
		e.compOf[k] = merged
	}
	if survivor != nil {
		merged.id = survivor.id
	} else {
		merged.id = e.newID(merged)
	}
	for _, c := range neighbors {
		if c.id != "" && c.id != merged.id {
			old := e.buildFireFromKeys(c.id, c.keys)
			changes = append(changes, Change{Kind: ChangeRetire, Fire: old, Reason: RetireMerged, MergedTo: merged.id})
		}
	}
	changes = append(changes, Change{Kind: ChangeUpsert, Fire: e.buildFire(merged)})
	return changes
}

func (e *Engine) remove(key string) []Change {
	comp := e.compOf[key]
	d := e.detections[key]
	before := e.buildFireFromKeys(comp.id, comp.keys)
	delete(e.detections, key)
	delete(e.compOf, key)
	e.unindex(d)
	delete(comp.keys, key)
	if len(comp.keys) == 0 {
		e.stats.Singletons--
		return nil
	}

	pieces := e.split(comp.keys)
	// The largest piece keeps the fire ID; ties go to the piece with the earliest anchor.
	sort.Slice(pieces, func(i, j int) bool {
		if len(pieces[i]) != len(pieces[j]) {
			return len(pieces[i]) > len(pieces[j])
		}
		return e.anchorLess(e.anchor(pieces[i]), e.anchor(pieces[j]))
	})

	var changes []Change
	for i, keys := range pieces {
		c := &component{keys: keys}
		for k := range keys {
			e.compOf[k] = c
		}
		if len(keys) == 1 {
			e.stats.Singletons++
		}
		switch {
		case i == 0 && comp.id != "" && len(keys) >= minFireDetections:
			c.id = comp.id
			changes = append(changes, Change{Kind: ChangeUpsert, Fire: e.buildFire(c)})
		case i == 0 && comp.id != "":
			changes = append(changes, Change{Kind: ChangeRetire, Fire: before, Reason: RetireBelowThreshold})
		case len(keys) >= minFireDetections:
			c.id = e.newID(c)
			changes = append(changes, Change{Kind: ChangeUpsert, Fire: e.buildFire(c)})
		}
	}
	return changes
}

// split returns the connected pieces of keys after a removal.
func (e *Engine) split(keys map[string]struct{}) []map[string]struct{} {
	visited := make(map[string]struct{}, len(keys))
	var pieces []map[string]struct{}
	for _, start := range sortedKeys(keys) {
		if _, ok := visited[start]; ok {
			continue
		}
		piece := map[string]struct{}{start: {}}
		visited[start] = struct{}{}
		queue := []string{start}
		for len(queue) > 0 {
			cur := e.detections[queue[0]]
			queue = queue[1:]
			for _, k := range e.neighborKeys(cur) {
				if _, in := keys[k]; !in {
					continue
				}
				if _, ok := visited[k]; ok {
					continue
				}
				visited[k] = struct{}{}
				piece[k] = struct{}{}
				queue = append(queue, k)
			}
		}
		pieces = append(pieces, piece)
	}
	return pieces
}

func (e *Engine) connected(a, b Detection) bool {
	gap := a.acquired.Sub(b.acquired)
	if gap < 0 {
		gap = -gap
	}
	return gap <= e.cfg.MaxGap && distanceKm(a, b) <= e.cfg.MaxDistanceKm
}

func stripOf(d Detection) int64 { return int64(math.Floor(d.lat / stripDegrees)) }

func (e *Engine) index(d Detection) {
	s := stripOf(d)
	if e.strips[s] == nil {
		e.strips[s] = make(map[string]struct{})
	}
	e.strips[s][d.Key()] = struct{}{}
}

func (e *Engine) unindex(d Detection) {
	s := stripOf(d)
	delete(e.strips[s], d.Key())
	if len(e.strips[s]) == 0 {
		delete(e.strips, s)
	}
}

// neighborKeys returns stored detections connected to d, excluding d itself.
func (e *Engine) neighborKeys(d Detection) []string {
	self := d.Key()
	var out []string
	s := stripOf(d)
	for strip := s - 1; strip <= s+1; strip++ {
		for k := range e.strips[strip] {
			if k != self && e.connected(d, e.detections[k]) {
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (e *Engine) neighborComponents(d Detection) []*component {
	seen := make(map[*component]struct{})
	var out []*component
	for _, k := range e.neighborKeys(d) {
		c := e.compOf[k]
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

// anchor is the earliest detection of a component (acquisition time, then key).
func (e *Engine) anchor(keys map[string]struct{}) Detection {
	var best Detection
	first := true
	for k := range keys {
		d := e.detections[k]
		if first || e.anchorLess(d, best) {
			best, first = d, false
		}
	}
	return best
}

func (e *Engine) anchorLess(a, b Detection) bool {
	if !a.acquired.Equal(b.acquired) {
		return a.acquired.Before(b.acquired)
	}
	return a.Key() < b.Key()
}

// newID derives a fire ID from the component's anchor detection, suffixed if that ID
// was ever used before so retired IDs are never reassigned.
func (e *Engine) newID(c *component) string {
	sum := sha256.Sum256([]byte(e.anchor(c.keys).Key()))
	base := "fire-" + hex.EncodeToString(sum[:8])
	id := base
	for n := 2; ; n++ {
		if _, used := e.usedIDs[id]; !used {
			break
		}
		id = fmt.Sprintf("%s-%d", base, n)
	}
	e.usedIDs[id] = struct{}{}
	return id
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
