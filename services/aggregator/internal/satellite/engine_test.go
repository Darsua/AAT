package satellite

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"bnpb/aggregator/internal/domain"
)

// 0.0089 degrees of latitude is about 0.99 km; 0.0091 is about 1.01 km.
const (
	justInside  = "0.0089"
	justOutside = "0.0091"
)

func det(t *testing.T, sat, date, hhmm, lat, lon, conf string, frp float64) Detection {
	t.Helper()
	d, err := NewDetection(sat, date, hhmm, lat, lon, conf, frp)
	if err != nil {
		t.Fatalf("NewDetection: %v", err)
	}
	return d
}

func addAll(t *testing.T, e *Engine, ds ...Detection) []Change {
	t.Helper()
	var all []Change
	for _, d := range ds {
		_, changes := e.Add(d)
		all = append(all, changes...)
	}
	return all
}

func fireKeySets(fires []Fire) []string {
	out := make([]string, 0, len(fires))
	for _, f := range fires {
		out = append(out, strings.Join(f.DetectionKeys, ","))
	}
	sort.Strings(out)
	return out
}

func TestTransitiveChainFormsOneFire(t *testing.T) {
	e := NewEngine(Config{})
	addAll(t, e,
		det(t, "N20", "2026-11-10", "0100", "-2.0000", "113.0", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "-2.0080", "113.0", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "-2.0160", "113.0", "n", 1),
	)
	fires := e.Fires()
	if len(fires) != 1 || len(fires[0].DetectionKeys) != 3 {
		t.Fatalf("fires = %+v, want one fire of 3 (end points are 1.78 km apart but linked through the middle)", fires)
	}
}

func TestCrossSatelliteDetectionsConnect(t *testing.T) {
	e := NewEngine(Config{})
	addAll(t, e,
		det(t, "N20", "2026-11-10", "0130", "-2.5", "113.8", "h", 12),
		det(t, "N21", "2026-11-10", "0215", "-2.5", "113.8", "n", 8),
	)
	fires := e.Fires()
	if len(fires) != 1 || !reflect.DeepEqual(fires[0].Satellites, []string{"N20", "N21"}) {
		t.Fatalf("fires = %+v, want one fire seen by N20 and N21", fires)
	}
	if fires[0].MaxConfidence != "h" || fires[0].TotalFRPMW != 20 {
		t.Fatalf("max_confidence/total_frp = %q/%v, want h/20", fires[0].MaxConfidence, fires[0].TotalFRPMW)
	}
}

func TestConnectivityBoundaries(t *testing.T) {
	cases := []struct {
		name       string
		lat2, time string
		connected  bool
	}{
		{"distance just inside 1 km", justInside, "0000", true},
		{"distance just outside 1 km", justOutside, "0000", false},
		{"exactly 360 minutes apart", "0", "0600", true},
		{"361 minutes apart", "0", "0601", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEngine(Config{})
			addAll(t, e,
				det(t, "N20", "2026-11-10", "0000", "0", "110", "n", 1),
				det(t, "N20", "2026-11-10", tc.time, tc.lat2, "110", "n", 1),
			)
			if got := len(e.Fires()) == 1; got != tc.connected {
				t.Fatalf("connected = %v, want %v", got, tc.connected)
			}
			if !tc.connected && e.Stats().Singletons != 2 {
				t.Fatalf("singletons = %d, want 2", e.Stats().Singletons)
			}
		})
	}
}

func TestDuplicatesCountOnceAndLowConfidenceIsExcluded(t *testing.T) {
	e := NewEngine(Config{})
	a := det(t, "N20", "2026-11-10", "0100", "-1.0", "101.0", "n", 5)
	b := det(t, "N20", "2026-11-10", "0110", "-1.001", "101.0", "n", 5)
	addAll(t, e, a, b)
	if disp, changes := e.Add(a); disp != DispositionDuplicate || changes != nil {
		t.Fatalf("duplicate add = %s, %v", disp, changes)
	}
	low := det(t, "N20", "2026-11-10", "0120", "-1.002", "101.0", "l", 50)
	if disp, _ := e.Add(low); disp != DispositionLowConfidence {
		t.Fatalf("low confidence add = %s", disp)
	}
	fires := e.Fires()
	if len(fires) != 1 || len(fires[0].DetectionKeys) != 2 || fires[0].TotalFRPMW != 10 {
		t.Fatalf("fires = %+v, want 2 detections and 10 MW", fires)
	}
	if s := e.Stats(); s.Duplicates != 1 || s.LowConfidence != 1 || s.Accepted != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestSeverityThresholds(t *testing.T) {
	cases := []struct {
		n    int
		frp  float64
		want domain.Severity
	}{
		{20, 0, domain.SeverityAwas},
		{2, 100, domain.SeverityAwas},
		{19, 99.9, domain.SeveritySiaga},
		{6, 0, domain.SeveritySiaga},
		{2, 30, domain.SeveritySiaga},
		{5, 29.9, domain.SeverityWaspada},
	}
	for _, tc := range cases {
		if got := Severity(tc.n, tc.frp); got != tc.want {
			t.Errorf("Severity(%d, %v) = %s, want %s", tc.n, tc.frp, got, tc.want)
		}
	}
}

func TestMergeKeepsLargerFireAndRetiresTheOther(t *testing.T) {
	e := NewEngine(Config{})
	addAll(t, e,
		det(t, "N20", "2026-11-10", "0100", "0.000", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.005", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.010", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.030", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.035", "110", "n", 1),
	)
	before := e.Fires()
	if len(before) != 2 {
		t.Fatalf("fires before bridge = %d, want 2", len(before))
	}
	large := before[0]
	if len(before[1].DetectionKeys) > len(large.DetectionKeys) {
		large = before[1]
	}

	changes := addAll(t, e,
		det(t, "N20", "2026-11-10", "0100", "0.018", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.026", "110", "n", 1),
	)
	fires := e.Fires()
	if len(fires) != 1 || fires[0].ID != large.ID || len(fires[0].DetectionKeys) != 7 {
		t.Fatalf("after bridge fires = %+v, want one fire %s with 7 detections", fires, large.ID)
	}
	var retired []Change
	for _, c := range changes {
		if c.Kind == ChangeRetire {
			retired = append(retired, c)
		}
	}
	if len(retired) != 1 || retired[0].Reason != RetireMerged || retired[0].MergedTo != large.ID {
		t.Fatalf("retirements = %+v, want the smaller fire merged into %s", retired, large.ID)
	}
}

func TestRetractSplitsAndShrinks(t *testing.T) {
	e := NewEngine(Config{})
	chain := []Detection{
		det(t, "N20", "2026-11-10", "0100", "0.000", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.008", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.016", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.024", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.032", "110", "n", 1),
	}
	addAll(t, e, chain...)
	original := e.Fires()[0].ID

	// Removing the middle splits 2+2; the piece with the earliest anchor keeps the ID.
	_, changes := e.Retract(chain[2].Key())
	fires := e.Fires()
	if len(fires) != 2 || len(changes) != 2 {
		t.Fatalf("after split fires = %+v, changes = %+v", fires, changes)
	}
	var kept, created Fire
	for _, f := range fires {
		if f.ID == original {
			kept = f
		} else {
			created = f
		}
	}
	if kept.ID == "" || kept.DetectionKeys[0] != chain[0].Key() || created.ID == "" {
		t.Fatalf("kept = %+v, created = %+v", kept, created)
	}

	// Shrinking below two detections retires the fire instead of leaving a stale hazard.
	_, changes = e.Retract(chain[0].Key())
	if len(changes) != 1 || changes[0].Kind != ChangeRetire || changes[0].Reason != RetireBelowThreshold || changes[0].Fire.ID != original {
		t.Fatalf("shrink changes = %+v", changes)
	}
	if disp, _ := e.Retract("unknown|key"); disp != DispositionUnknown {
		t.Fatalf("unknown retract = %s", disp)
	}
}

func TestUpdateRevisesExistingFire(t *testing.T) {
	e := NewEngine(Config{})
	a := det(t, "N20", "2026-11-10", "0100", "0.000", "110", "n", 10)
	b := det(t, "N20", "2026-11-10", "0100", "0.005", "110", "n", 10)
	addAll(t, e, a, b)
	id := e.Fires()[0].ID

	b2 := det(t, "N20", "2026-11-10", "0100", "0.005", "110", "h", 95)
	disp, changes := e.Update(b2)
	if disp != DispositionUpdated || len(changes) != 1 || changes[0].Fire.ID != id {
		t.Fatalf("update = %s, %+v", disp, changes)
	}
	if f := changes[0].Fire; f.TotalFRPMW != 105 || f.Severity != domain.SeverityAwas || f.MaxConfidence != "h" {
		t.Fatalf("updated fire = %+v", f)
	}

	// Downgrading to low confidence removes the detection and retires the fire.
	_, changes = e.Update(det(t, "N20", "2026-11-10", "0100", "0.005", "110", "l", 95))
	if len(changes) != 1 || changes[0].Kind != ChangeRetire {
		t.Fatalf("low-confidence update changes = %+v", changes)
	}
}

func TestLateDetectionsAreRecordedNotDropped(t *testing.T) {
	e := NewEngine(Config{})
	addAll(t, e, det(t, "N20", "2026-11-10", "1200", "0", "110", "n", 1))
	inWindow := det(t, "N20", "2026-11-10", "0700", "0.001", "110", "n", 1)
	if disp, _ := e.Add(inWindow); disp != DispositionAccepted {
		t.Fatalf("in-window late arrival = %s, want accepted", disp)
	}
	beyond := det(t, "N20", "2026-11-10", "0500", "0.002", "110", "n", 1)
	if disp, _ := e.Add(beyond); disp != DispositionLate {
		t.Fatalf("beyond-watermark arrival = %s, want late", disp)
	}
	if e.Stats().Late != 1 || len(e.Late()) != 1 || e.Late()[0].Key() != beyond.Key() {
		t.Fatalf("late stats = %+v, late = %+v", e.Stats(), e.Late())
	}
	if disp, _ := e.Add(beyond); disp != DispositionDuplicate {
		t.Fatalf("repeated late arrival = %s, want duplicate", disp)
	}
}

func TestClustersDoNotDependOnArrivalOrder(t *testing.T) {
	ds := []Detection{
		det(t, "N20", "2026-11-10", "0100", "0.000", "110", "n", 1),
		det(t, "N21", "2026-11-10", "0300", "0.008", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0500", "0.016", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.500", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0130", "0.504", "110", "n", 1),
		det(t, "N20", "2026-11-10", "0100", "0.900", "110", "n", 1),
	}
	forward := NewEngine(Config{})
	addAll(t, forward, ds...)
	reverse := NewEngine(Config{})
	for i := len(ds) - 1; i >= 0; i-- {
		reverse.Add(ds[i])
	}
	if a, b := fireKeySets(forward.Fires()), fireKeySets(reverse.Fires()); !reflect.DeepEqual(a, b) {
		t.Fatalf("forward %v != reverse %v", a, b)
	}
}

func TestSnapshotRestoreMatchesUninterruptedRun(t *testing.T) {
	events := []func(e *Engine) []Change{}
	add := func(d Detection) {
		events = append(events, func(e *Engine) []Change { _, c := e.Add(d); return c })
	}
	retract := func(key string) {
		events = append(events, func(e *Engine) []Change { _, c := e.Retract(key); return c })
	}
	chain := []Detection{
		det(t, "N20", "2026-11-10", "0100", "0.000", "110", "n", 4),
		det(t, "N21", "2026-11-10", "0200", "0.008", "110", "h", 9),
		det(t, "N20", "2026-11-10", "0300", "0.016", "110", "n", 7),
		det(t, "N20", "2026-11-10", "0400", "0.024", "110", "n", 2),
	}
	for _, d := range chain {
		add(d)
	}
	add(det(t, "N20", "2026-11-10", "0400", "0.300", "110", "n", 1))
	retract(chain[2].Key())
	add(det(t, "N21", "2026-11-10", "0410", "0.302", "110", "n", 1))
	add(det(t, "N20", "2026-11-10", "0420", "0.016", "110", "n", 3))

	uninterrupted := NewEngine(Config{})
	var want []Change
	for _, ev := range events {
		want = append(want, ev(uninterrupted)...)
	}

	for cut := 0; cut <= len(events); cut++ {
		first := NewEngine(Config{})
		var got []Change
		for _, ev := range events[:cut] {
			got = append(got, ev(first)...)
		}
		raw, err := json.Marshal(first.Snapshot())
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		var st State
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatalf("unmarshal snapshot: %v", err)
		}
		resumed, err := Restore(Config{}, st)
		if err != nil {
			t.Fatalf("restore at cut %d: %v", cut, err)
		}
		for _, ev := range events[cut:] {
			got = append(got, ev(resumed)...)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("cut %d: changes differ\n got: %+v\nwant: %+v", cut, got, want)
		}
		if !reflect.DeepEqual(resumed.Fires(), uninterrupted.Fires()) || resumed.Stats() != uninterrupted.Stats() {
			t.Fatalf("cut %d: final state differs", cut)
		}
	}
}

func TestParseDetectionKeepsRawCoordinatesAndMapsHazard(t *testing.T) {
	d, err := ParseDetection([]byte(`{"satellite":"N20","acq_date":"2026-11-10","acq_time":905,"latitude":-2.51230,"longitude":113.80440,"confidence":"h","frp":12.5}`))
	if err != nil {
		t.Fatalf("ParseDetection: %v", err)
	}
	if got, want := d.Key(), "N20|2026-11-10|905|-2.51230|113.80440"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
	if got := d.AcquiredAt().Format("2006-01-02T15:04Z"); got != "2026-11-10T09:05Z" {
		t.Fatalf("acquired = %s", got)
	}
	for _, bad := range []string{
		`{"satellite":"N20","acq_date":"2026-11-10","acq_time":"2460","latitude":0,"longitude":0}`,
		`{"satellite":"N20","acq_date":"10/11/2026","acq_time":"0100","latitude":0,"longitude":0}`,
		`{"satellite":"N20","acq_date":"2026-11-10","acq_time":"0100","latitude":91,"longitude":0}`,
		`{"satellite":"","acq_date":"2026-11-10","acq_time":"0100","latitude":0,"longitude":0}`,
		`{"satellite":"N20","acq_date":"2026-11-10","acq_time":"0100","latitude":0,"longitude":0,"frp":-1}`,
		`not json`,
	} {
		if _, err := ParseDetection([]byte(bad)); err == nil {
			t.Errorf("ParseDetection(%s) succeeded, want error", bad)
		}
	}

	e := NewEngine(Config{})
	addAll(t, e, d, det(t, "N21", "2026-11-10", "1000", "-2.51300", "113.80440", "n", 7.5))
	h := e.Fires()[0].ToHazard(func(lat, lon float64) string { return "Kalimantan Tengah" })
	if h.Source != SourceSatelit || h.HazardType != HazardTypeWildfire || h.HazardID != "haz-satelit-"+h.SourceRefID || h.AreaName != "Kalimantan Tengah" {
		t.Fatalf("hazard = %+v", h)
	}
	if h.Attributes["detection_count"] != 2 || h.Attributes["total_frp_mw"] != 20.0 || h.Attributes["max_confidence"] != "h" {
		t.Fatalf("attributes = %+v", h.Attributes)
	}
	if !h.OccurredAt.Equal(d.AcquiredAt()) {
		t.Fatalf("occurred_at = %s, want earliest detection %s", h.OccurredAt, d.AcquiredAt())
	}
}
