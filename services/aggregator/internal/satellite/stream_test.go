package satellite

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func detectionData(i int64) string {
	return fmt.Sprintf(`{"satellite":"N20","acq_date":"2026-11-10","acq_time":"0100","latitude":"0.%03d","longitude":"110","confidence":"n","frp":1}`, i)
}

func writeEvent(w http.ResponseWriter, offset int64) {
	fmt.Fprintf(w, "id: %d\nevent: detection\ndata: %s\n\n", offset, detectionData(offset))
}

// feedServer serves offsets per connection from a script; when the script runs out it
// keeps the stream open until the client disconnects.
type feedServer struct {
	mu        sync.Mutex
	inclusive bool
	scripts   [][]int64 // offsets sent on connection i; nil means "from..last"
	last      int64
	conns     []string // "from" query seen per connection
	status    []int    // optional HTTP status per connection
}

func (f *feedServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	n := len(f.conns)
	f.conns = append(f.conns, r.URL.Query().Get("from"))
	var script []int64
	if n < len(f.scripts) {
		script = f.scripts[n]
	}
	status := http.StatusOK
	if n < len(f.status) && f.status[n] != 0 {
		status = f.status[n]
	}
	f.mu.Unlock()

	if r.Header.Get("Authorization") != "Bearer group-token" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if status != http.StatusOK {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	if script == nil {
		start := from + 1
		if f.inclusive {
			start = from
		}
		for o := start; o <= f.last; o++ {
			if o > 0 {
				script = append(script, o)
			}
		}
	}
	fmt.Fprint(w, ": comment\nevent: heartbeat\ndata: {}\n\n")
	for _, o := range script {
		writeEvent(w, o)
	}
	w.(http.Flusher).Flush()
	if n < len(f.scripts) {
		return // scripted connections drop after sending
	}
	<-r.Context().Done()
}

func runUntil(t *testing.T, f *feedServer, lastApplied, stopAt int64) ([]int64, error) {
	t.Helper()
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := NewClient(srv.URL, "group-token")
	c.MinBackoff, c.MaxBackoff = time.Millisecond, 5*time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []int64
	err := c.Run(ctx, lastApplied, func(ev Event) error {
		got = append(got, ev.Offset)
		if ev.Offset == stopAt {
			cancel()
		}
		return nil
	})
	return got, err
}

func TestRunResumesWithoutLossOrDuplicates(t *testing.T) {
	for _, inclusive := range []bool{false, true} {
		t.Run(fmt.Sprintf("inclusive=%v", inclusive), func(t *testing.T) {
			f := &feedServer{inclusive: inclusive, last: 6, scripts: [][]int64{{1, 2, 3}}}
			got, err := runUntil(t, f, 0, 6)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v", err)
			}
			if want := []int64{1, 2, 3, 4, 5, 6}; !reflect.DeepEqual(got, want) {
				t.Fatalf("offsets = %v, want %v", got, want)
			}
			if f.conns[1] != "3" {
				t.Fatalf("reconnect from = %q, want 3", f.conns[1])
			}
		})
	}
}

func TestRunReconnectsOnGapInsteadOfSkipping(t *testing.T) {
	f := &feedServer{last: 3, scripts: [][]int64{{1, 3}}}
	got, err := runUntil(t, f, 0, 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	if want := []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("offsets = %v, want %v", got, want)
	}
}

func TestRunRetriesUnavailableAndStopsOnUnauthorized(t *testing.T) {
	f := &feedServer{last: 2, status: []int{http.StatusServiceUnavailable, http.StatusTooManyRequests}}
	got, err := runUntil(t, f, 0, 2)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("after 503/429: offsets = %v, err = %v", got, err)
	}

	srv := httptest.NewServer(&feedServer{last: 2})
	defer srv.Close()
	err = NewClient(srv.URL, "wrong-token").Run(context.Background(), 0, func(Event) error { return nil })
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong token error = %v, want ErrUnauthorized", err)
	}
}

func TestRunStopsWhenHandlerFails(t *testing.T) {
	srv := httptest.NewServer(&feedServer{last: 3})
	defer srv.Close()
	boom := errors.New("persist failed")
	var got []int64
	err := NewClient(srv.URL, "group-token").Run(context.Background(), 0, func(ev Event) error {
		if ev.Offset == 2 {
			return boom
		}
		got = append(got, ev.Offset)
		return nil
	})
	if !errors.Is(err, boom) || !reflect.DeepEqual(got, []int64{1}) {
		t.Fatalf("offsets = %v, err = %v", got, err)
	}
}

func TestReadSSEJoinsDataLines(t *testing.T) {
	body := "id: 7\nevent: retract\ndata: {\"detection_key\":\ndata: \"N20|k\"}\n\n"
	var got []Event
	if err := readSSE(strings.NewReader(body), func(ev Event) error { got = append(got, ev); return nil }); err != nil {
		t.Fatalf("readSSE: %v", err)
	}
	if len(got) != 1 || got[0].Offset != 7 || got[0].Type != EventRetract || string(got[0].Data) != "{\"detection_key\":\n\"N20|k\"}" {
		t.Fatalf("events = %+v", got)
	}
}

func TestApplyEventRetractByKeyOrFields(t *testing.T) {
	e := NewEngine(Config{})
	for i := int64(1); i <= 3; i++ {
		if _, _, err := ApplyEvent(e, Event{Offset: i, Type: EventDetection, Data: []byte(detectionData(i))}); err != nil {
			t.Fatalf("apply detection: %v", err)
		}
	}
	d1, _ := ParseDetection([]byte(detectionData(1)))
	if disp, _, err := ApplyEvent(e, Event{Type: EventRetract, Data: []byte(`{"detection_key":"` + d1.Key() + `"}`)}); err != nil || disp != DispositionRetracted {
		t.Fatalf("retract by key = %s, %v", disp, err)
	}
	if disp, _, err := ApplyEvent(e, Event{Type: EventRetract, Data: []byte(detectionData(2))}); err != nil || disp != DispositionRetracted {
		t.Fatalf("retract by fields = %s, %v", disp, err)
	}
	if _, _, err := ApplyEvent(e, Event{Type: "bogus"}); err == nil {
		t.Fatal("unsupported event type should fail")
	}
	if _, _, err := ApplyEvent(e, Event{Type: EventDetection, Data: []byte(`{"satellite":`)}); err == nil {
		t.Fatal("truncated detection should fail")
	}
}

func TestGetSession(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/session" || r.Header.Get("Authorization") != "Bearer group-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprint(w, `{"group":"17","profile":"exam","speed":60,"virtual_now":"2026-11-10T03:00:00Z","latest_offset":42}`)
	}))
	defer srv.Close()
	s, err := NewClient(srv.URL, "group-token").GetSession(context.Background())
	if err != nil || s.LatestOffset != 42 || s.Speed != 60 {
		t.Fatalf("session = %+v, err = %v", s, err)
	}
}
