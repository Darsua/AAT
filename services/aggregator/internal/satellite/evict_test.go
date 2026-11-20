package satellite

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func evictDetection(t *testing.T, sat string, at time.Time, lat, lon float64, frp float64) Detection {
	t.Helper()
	d, err := NewDetection(sat, at.Format("2006-01-02"), at.Format("1504"),
		fmt.Sprintf("%.5f", lat), fmt.Sprintf("%.5f", lon), "n", frp)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// persisted replays changes the way the worker stores them: the last change per fire wins.
func persisted(into map[string]string, changes []Change) {
	for _, c := range changes {
		into[c.Fire.ID] = fmt.Sprintf("%s keys=%v frp=%.2f sev=%s", c.Kind, c.Fire.DetectionKeys, c.Fire.TotalFRPMW, c.Fire.Severity)
	}
}

func TestEvictClosesOnlyFiresThatCannotGrow(t *testing.T) {
	e := NewEngine(Config{})
	day1 := time.Date(2026, 11, 27, 1, 0, 0, 0, time.UTC)
	old1 := evictDetection(t, "N20", day1, -2.0, 113.5, 10)
	old2 := evictDetection(t, "N21", day1.Add(30*time.Minute), -2.003, 113.5, 10)
	for _, d := range []Detection{old1, old2} {
		e.Add(d)
	}
	low := evictDetection(t, "N", day1, -5, 120, 1)
	low.Confidence = "l"
	e.Add(low)

	// Within the retention window nothing is evicted.
	day2 := day1.Add(20 * time.Hour)
	e.Add(evictDetection(t, "N20", day2, 1.0, 101.0, 5))
	if closed, dropped := e.Evict(24 * time.Hour); len(closed) != 0 || dropped != 0 {
		t.Fatalf("evicted inside the window: %v %d", closed, dropped)
	}

	// A day later the first fire can no longer grow and is closed.
	firstID := e.Fires()[0].ID
	day3 := day1.Add(30 * time.Hour)
	e.Add(evictDetection(t, "N20", day3, 1.0, 101.0, 5))
	e.Add(evictDetection(t, "N21", day3.Add(10*time.Minute), 1.002, 101.0, 5))
	closed, dropped := e.Evict(24 * time.Hour)
	if !reflect.DeepEqual(closed, []string{firstID}) || dropped != 3 {
		t.Fatalf("closed=%v dropped=%d, want [%s] and 3 (2 fire + 1 low)", closed, dropped, firstID)
	}
	if fires := e.Fires(); len(fires) != 1 || fires[0].ID == firstID {
		t.Fatalf("fires after evict = %+v", fires)
	}
	if st := e.Stats(); st.ClosedFires != 1 || st.Evicted != 3 {
		t.Fatalf("stats = %+v", st)
	}

	// Late references to evicted detections never reopen the fire.
	if disp, changes := e.Add(old1); disp != DispositionLate || len(changes) != 0 {
		t.Fatalf("re-delivered evicted detection = %s %v", disp, changes)
	}
	if disp, changes := e.Retract(old2.Key()); disp != DispositionUnknown || len(changes) != 0 {
		t.Fatalf("retract of evicted detection = %s %v", disp, changes)
	}

	// The evicted state survives a snapshot round trip.
	restored, err := Restore(Config{}, e.Snapshot())
	if err != nil || !reflect.DeepEqual(restored.Snapshot(), e.Snapshot()) {
		t.Fatalf("restore after evict: %v", err)
	}
}

func TestEvictNeverChangesPersistedFires(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	base := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)
	type step struct {
		d       Detection
		retract bool
	}
	var steps []step
	for f := 0; f < 300; f++ { // fires spread over five days
		lat, lon := rng.Float64()*6-4, 100+rng.Float64()*16
		start := base.Add(time.Duration(rng.Intn(5*24*60)) * time.Minute)
		for k := 0; k < 2+rng.Intn(12); k++ {
			lat += rng.Float64()*0.006 - 0.003
			lon += rng.Float64()*0.006 - 0.003
			at := start.Add(time.Duration(k*(5+rng.Intn(40))) * time.Minute)
			steps = append(steps, step{d: evictDetection(t, []string{"N", "N20", "N21"}[rng.Intn(3)], at, lat, lon, rng.Float64()*20)})
		}
	}
	// Mostly in time order, with some events up to a few hours late and some retracts.
	for i := range steps {
		j := i + rng.Intn(20)
		if j < len(steps) && rng.Intn(10) == 0 {
			steps[i], steps[j] = steps[j], steps[i]
		}
	}
	// Retracts follow their detection within a few dozen events, i.e. inside the
	// retention window; references older than retention are covered above.
	var withRetracts []step
	for i, s := range steps {
		withRetracts = append(withRetracts, s)
		if i%37 == 0 && i >= 30 {
			withRetracts = append(withRetracts, step{d: steps[i-rng.Intn(30)].d, retract: true})
		}
	}
	steps = withRetracts

	plain, evicting := NewEngine(Config{}), NewEngine(Config{})
	want, got := map[string]string{}, map[string]string{}
	totalClosed := 0
	for _, s := range steps {
		if s.retract {
			_, c := plain.Retract(s.d.Key())
			persisted(want, c)
			_, c = evicting.Retract(s.d.Key())
			persisted(got, c)
		} else {
			_, c := plain.Add(s.d)
			persisted(want, c)
			_, c = evicting.Add(s.d)
			persisted(got, c)
		}
		closed, _ := evicting.Evict(evicting.MinRetention())
		totalClosed += len(closed)
	}
	if totalClosed == 0 {
		t.Fatal("test stream never triggered eviction")
	}
	if !reflect.DeepEqual(got, want) {
		for id, w := range want {
			if got[id] != w {
				t.Errorf("fire %s:\n got %s\nwant %s", id, got[id], w)
			}
		}
		t.Fatalf("eviction changed %d persisted fires", len(want))
	}
	if len(evicting.Snapshot().Detections) >= len(plain.Snapshot().Detections) {
		t.Fatalf("eviction kept %d of %d detections", len(evicting.Snapshot().Detections), len(plain.Snapshot().Detections))
	}
}
