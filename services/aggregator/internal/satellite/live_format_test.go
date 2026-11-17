package satellite

import (
	"reflect"
	"strconv"
	"testing"
)

// Payload shapes as observed on the live feed (2026-11-17).
func liveDetection(offset int64, sat, date string, hhmm int, lat, lon, conf string, frp float64) Event {
	data := `{"offset":` + itoa(offset) + `,"detection":{"version":"2.0NRT","scan":0.49,"acq_date":"` + date +
		`","acq_time":` + itoa(int64(hhmm)) + `,"bright_ti4":317.96,"bright_ti5":325.05,"confidence":"` + conf +
		`","daynight":"D","frp":` + ftoa(frp) + `,"instrument":"VIIRS","latitude":` + lat + `,"longitude":` + lon +
		`,"satellite":"` + sat + `","track":0.55}}`
	return Event{Offset: offset, Type: EventDetection, Data: []byte(data)}
}

func itoa(i int64) string   { return strconv.FormatInt(i, 10) }
func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func TestApplyEventLiveDetectionEnvelope(t *testing.T) {
	e := NewEngine(Config{})
	disp, _, err := ApplyEvent(e, liveDetection(1, "N21", "2026-11-27", 2352, "1.12035", "115.03373", "n", 2.8))
	if err != nil || disp != DispositionAccepted {
		t.Fatalf("detection = %s, %v", disp, err)
	}
	// acq_time arrives as a number and keeps its unpadded text, matching the feed's own keys.
	if _, ok := e.Lookup("N21|2026-11-27|2352|1.12035|115.03373"); !ok {
		t.Fatalf("key not found; have %v", e.Snapshot().Detections)
	}
	disp, _, err = ApplyEvent(e, liveDetection(2, "N20", "2026-11-28", 5, "1.12233", "115.03378", "n", 1))
	if err != nil || disp != DispositionAccepted {
		t.Fatalf("early-hour detection = %s, %v", disp, err)
	}
	if _, ok := e.Lookup("N20|2026-11-28|5|1.12233|115.03378"); !ok {
		t.Fatal("unpadded acq_time key not found")
	}
}

func TestApplyEventLiveUpdatePatchesByKey(t *testing.T) {
	e := NewEngine(Config{})
	for _, ev := range []Event{
		liveDetection(0, "N", "2026-11-27", 2340, "1.12073", "115.03242", "h", 9.42),
		liveDetection(1, "N", "2026-11-27", 2352, "1.12241", "115.03181", "n", 2.8),
		liveDetection(2, "N21", "2026-11-27", 2352, "1.12035", "115.03373", "l", 2.45),
	} {
		if _, _, err := ApplyEvent(e, ev); err != nil {
			t.Fatal(err)
		}
	}
	if fires := e.Fires(); len(fires) != 1 || len(fires[0].DetectionKeys) != 2 {
		t.Fatalf("fires = %+v", fires)
	}

	// Raising the low-confidence detection's confidence brings it into the fire.
	raise := Event{Offset: 3, Type: EventUpdate, Data: []byte(`{"offset":3,"key":"N21|2026-11-27|2352|1.12035|115.03373","changes":{"confidence":"n","frp":4.5}}`)}
	disp, changes, err := ApplyEvent(e, raise)
	if err != nil || disp != DispositionUpdated || len(changes) != 1 || len(changes[0].Fire.DetectionKeys) != 3 {
		t.Fatalf("raise = %s %+v %v", disp, changes, err)
	}

	// Dropping a member to low confidence removes it; the fire shrinks but survives.
	drop := Event{Offset: 4, Type: EventUpdate, Data: []byte(`{"offset":4,"key":"N|2026-11-27|2352|1.12241|115.03181","changes":{"bright_ti5":274.48,"confidence":"l"}}`)}
	disp, changes, err = ApplyEvent(e, drop)
	if err != nil || disp != DispositionUpdated || len(changes) != 1 || len(changes[0].Fire.DetectionKeys) != 2 {
		t.Fatalf("drop = %s %+v %v", disp, changes, err)
	}

	// A brightness-only update changes nothing that matters and emits nothing for a low detection.
	bright := Event{Offset: 5, Type: EventUpdate, Data: []byte(`{"offset":5,"key":"N|2026-11-27|2352|1.12241|115.03181","changes":{"bright_ti5":272.96}}`)}
	if disp, changes, err = ApplyEvent(e, bright); err != nil || disp != DispositionUpdated || len(changes) != 0 {
		t.Fatalf("brightness = %s %+v %v", disp, changes, err)
	}

	// An update for a key never seen is counted, not fatal.
	unknown := Event{Offset: 6, Type: EventUpdate, Data: []byte(`{"offset":6,"key":"N20|2026-11-28|5|0.1|0.1","changes":{"frp":1}}`)}
	if disp, _, err = ApplyEvent(e, unknown); err != nil || disp != DispositionUnknown {
		t.Fatalf("unknown = %s %v", disp, err)
	}

	// Identity fields cannot be patched.
	bad := Event{Offset: 7, Type: EventUpdate, Data: []byte(`{"offset":7,"key":"N|2026-11-27|2340|1.12073|115.03242","changes":{"latitude":2}}`)}
	if _, _, err = ApplyEvent(e, bad); err == nil {
		t.Fatal("identity change accepted")
	}

	// Low-confidence detections survive a snapshot so later patches still find them.
	restored, err := Restore(Config{}, e.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Snapshot(), e.Snapshot()) {
		t.Fatal("snapshot did not round-trip")
	}
	if _, ok := restored.Lookup("N|2026-11-27|2352|1.12241|115.03181"); !ok {
		t.Fatal("low-confidence detection lost on restore")
	}

	// Retracting a low-confidence detection by key works too.
	retract := Event{Offset: 8, Type: EventRetract, Data: []byte(`{"offset":8,"key":"N|2026-11-27|2352|1.12241|115.03181"}`)}
	if disp, _, err = ApplyEvent(restored, retract); err != nil || disp != DispositionRetracted {
		t.Fatalf("retract low = %s %v", disp, err)
	}
}

func TestRunStartsAtOffsetZero(t *testing.T) {
	f := &feedServer{scripts: [][]int64{{0, 1, 2}}}
	got, _ := runUntil(t, f, -1, 2)
	if !reflect.DeepEqual(got, []int64{0, 1, 2}) {
		t.Fatalf("got %v, want [0 1 2]", got)
	}
	if f.conns[0] != "0" {
		t.Fatalf("first request from=%q, want 0", f.conns[0])
	}
}
