package satellite

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Feed event types from issue #28.
const (
	EventDetection = "detection"
	EventUpdate    = "update"
	EventRetract   = "retract"
	EventHeartbeat = "heartbeat"
)

var (
	// ErrUnauthorized means the group token was rejected; retrying cannot help.
	ErrUnauthorized = errors.New("satellite feed rejected the token")
	// ErrBadRequest means the feed rejected the request (for example an invalid offset).
	ErrBadRequest = errors.New("satellite feed rejected the request")
	// errGap means an offset was skipped; the stream is reopened from the last applied offset.
	errGap = errors.New("satellite feed skipped an offset")
)

// Event is one server-sent event with its feed offset.
type Event struct {
	Offset int64
	Type   string
	Data   []byte
}

// Session is the GET /v1/session response. Field names follow issue #28 and are not yet
// checked against the live feed.
type Session struct {
	Group        string    `json:"group"`
	Profile      string    `json:"profile"`
	Speed        float64   `json:"speed"`
	VirtualNow   time.Time `json:"virtual_now"`
	LatestOffset int64     `json:"latest_offset"`
}

// Client reads the satellite SSE feed.
type Client struct {
	BaseURL    string
	Token      string
	HTTP       *http.Client
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// OnReconnect, if set, is called before each reconnect with the reason and the wait.
	OnReconnect func(err error, lastApplied int64, wait time.Duration)
}

// NewClient returns a client with streaming-safe defaults (bounded header wait, no overall stream timeout).
func NewClient(baseURL, token string) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		Token:      token,
		HTTP:       &http.Client{Transport: transport},
		MinBackoff: 500 * time.Millisecond,
		MaxBackoff: 30 * time.Second,
	}
}

func (c *Client) newRequest(ctx context.Context, path string, query url.Values) (*http.Request, error) {
	u := c.BaseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	return req, nil
}

// statusError classifies a non-200 response. Retryable errors carry the server's
// Retry-After hint when present.
type statusError struct {
	code       int
	retryAfter time.Duration
}

func (e *statusError) Error() string { return fmt.Sprintf("satellite feed returned HTTP %d", e.code) }

func checkStatus(resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return ErrUnauthorized
	case http.StatusBadRequest:
		return ErrBadRequest
	}
	se := &statusError{code: resp.StatusCode}
	if secs, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && secs > 0 {
		se.retryAfter = time.Duration(secs) * time.Second
	}
	return se
}

// GetSession calls GET /v1/session.
func (c *Client) GetSession(ctx context.Context) (Session, error) {
	req, err := c.newRequest(ctx, "/v1/session", nil)
	if err != nil {
		return Session{}, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return Session{}, err
	}
	var s Session
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&s); err != nil {
		return Session{}, fmt.Errorf("decode session: %w", err)
	}
	return s, nil
}

// Run streams events after lastApplied, calling handle once per offset in order, and
// reconnects on transient failures until ctx ends or a fatal error occurs.
//
// Resume is boundary-agnostic: it requests from=lastApplied and drops any offset at or
// below lastApplied, so it neither loses nor repeats events whether the feed treats
// "from" as inclusive or exclusive. A skipped offset forces a reconnect instead of
// silently advancing. handle must durably apply the event before returning nil; an error
// from handle stops Run without advancing.
func (c *Client) Run(ctx context.Context, lastApplied int64, handle func(Event) error) error {
	backoff := c.MinBackoff
	for {
		progressed, err := c.streamOnce(ctx, &lastApplied, handle)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrBadRequest):
			return err
		case err != nil && !isTransient(err):
			return err
		}
		if progressed {
			backoff = c.MinBackoff
		}
		wait := backoff
		var se *statusError
		if errors.As(err, &se) && se.retryAfter > wait {
			wait = se.retryAfter
		}
		if c.OnReconnect != nil {
			c.OnReconnect(err, lastApplied, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > c.MaxBackoff {
			backoff = c.MaxBackoff
		}
	}
}

// handlerError wraps failures from the caller's handler so Run does not retry them.
type handlerError struct{ err error }

func (e handlerError) Error() string { return e.err.Error() }
func (e handlerError) Unwrap() error { return e.err }

func isTransient(err error) bool {
	var he handlerError
	return !errors.As(err, &he)
}

func (c *Client) streamOnce(ctx context.Context, lastApplied *int64, handle func(Event) error) (bool, error) {
	// Offsets start at 0, so a negative lastApplied means nothing has been applied yet.
	// from= is honoured as inclusive by the feed; already-applied offsets are dropped below.
	from := *lastApplied
	if from < 0 {
		from = 0
	}
	q := url.Values{"from": {strconv.FormatInt(from, 10)}}
	req, err := c.newRequest(ctx, "/v1/stream", q)
	if err != nil {
		return false, handlerError{err}
	}
	req.Header.Set("Accept", "text/event-stream")
	if *lastApplied >= 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(*lastApplied, 10))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return false, err
	}

	progressed := false
	err = readSSE(resp.Body, func(ev Event) error {
		// Heartbeats with an id occupy an offset on the live feed (2026-11-17), so they
		// advance the position like any other event; id-less heartbeats are ignored.
		if (ev.Type == EventHeartbeat && ev.Offset < 0) || ev.Offset <= *lastApplied {
			return nil
		}
		if ev.Offset != *lastApplied+1 {
			return fmt.Errorf("%w: expected %d, got %d", errGap, *lastApplied+1, ev.Offset)
		}
		if err := handle(ev); err != nil {
			return handlerError{err}
		}
		*lastApplied = ev.Offset
		progressed = true
		return nil
	})
	if err == nil {
		err = io.ErrUnexpectedEOF // the feed is endless; a clean close is a disconnect
	}
	return progressed, err
}

// maxSSELine bounds a single SSE line so a malformed feed cannot exhaust memory.
const maxSSELine = 1 << 20

// readSSE parses a text/event-stream body. Events without a numeric id are ignored
// unless they are heartbeats.
func readSSE(body io.Reader, emit func(Event) error) error {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), maxSSELine)
	var (
		id, typ string
		data    []string
	)
	dispatch := func() error {
		defer func() { id, typ, data = "", "", nil }()
		if len(data) == 0 && typ == "" {
			return nil
		}
		if typ == "" {
			typ = "message"
		}
		ev := Event{Type: typ, Data: []byte(strings.Join(data, "\n"))}
		off, err := strconv.ParseInt(id, 10, 64)
		switch {
		case err == nil:
			ev.Offset = off
		case typ == EventHeartbeat:
			ev.Offset = -1 // id-less heartbeat: keep-alive only
		default:
			return nil
		}
		return emit(ev)
	}
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			id = value
		case "event":
			typ = value
		case "data":
			data = append(data, value)
		}
	}
	return sc.Err()
}

// envelope is the feed's event payload. Observed on the live feed (2026-11-17):
//
//	detection: {"offset":0,"detection":{"satellite":"N21","acq_date":"2026-11-27","acq_time":2352,...}}
//	update:    {"offset":36,"key":"N21|2026-11-27|2352|1.12035|115.03373","changes":{"confidence":"l","frp":2.45}}
//
// Retracts are expected to carry the same "key". A bare detection object, or
// "detection_key", is still accepted.
type envelope struct {
	Detection    json.RawMessage            `json:"detection"`
	Key          string                     `json:"key"`
	DetectionKey string                     `json:"detection_key"`
	Changes      map[string]json.RawMessage `json:"changes"`
}

// identityFields cannot change through an update: they make up the detection key.
var identityFields = map[string]bool{"satellite": true, "acq_date": true, "acq_time": true, "latitude": true, "longitude": true}

// ApplyEvent applies a feed event to the engine.
func ApplyEvent(e *Engine, ev Event) (Disposition, []Change, error) {
	if ev.Type == EventHeartbeat {
		return DispositionHeartbeat, nil, nil
	}
	var env envelope
	if err := json.Unmarshal(ev.Data, &env); err != nil {
		return "", nil, fmt.Errorf("decode %s: %w", ev.Type, err)
	}
	key := env.Key
	if key == "" {
		key = env.DetectionKey
	}
	switch ev.Type {
	case EventDetection:
		payload := ev.Data
		if len(env.Detection) > 0 {
			payload = env.Detection
		}
		d, err := ParseDetection(payload)
		if err != nil {
			return "", nil, err
		}
		disp, changes := e.Add(d)
		return disp, changes, nil
	case EventUpdate:
		if key == "" {
			// Full-record update without a key.
			d, err := ParseDetection(ev.Data)
			if err != nil {
				return "", nil, fmt.Errorf("update without key: %w", err)
			}
			disp, changes := e.Update(d)
			return disp, changes, nil
		}
		base, ok := e.Lookup(key)
		if !ok {
			e.stats.Unknown++
			return DispositionUnknown, nil, nil
		}
		d, err := patchDetection(base, env.Changes)
		if err != nil {
			return "", nil, fmt.Errorf("update %s: %w", key, err)
		}
		disp, changes := e.Update(d)
		return disp, changes, nil
	case EventRetract:
		if key == "" {
			payload := ev.Data
			if len(env.Detection) > 0 {
				payload = env.Detection
			}
			d, err := ParseDetection(payload)
			if err != nil {
				return "", nil, fmt.Errorf("retract without key: %w", err)
			}
			key = d.Key()
		}
		disp, changes := e.Retract(key)
		return disp, changes, nil
	}
	return "", nil, fmt.Errorf("unsupported event type %q", ev.Type)
}

// patchDetection applies an update's changed fields to the stored detection. Fields the
// engine does not use (brightness, scan, track) are ignored; identity fields may not change.
func patchDetection(base Detection, changes map[string]json.RawMessage) (Detection, error) {
	for field := range changes {
		if identityFields[field] {
			return Detection{}, fmt.Errorf("identity field %q cannot change", field)
		}
	}
	confidence, frp := base.Confidence, base.FRP
	if raw, ok := changes["confidence"]; ok {
		c, err := scalarText(raw)
		if err != nil {
			return Detection{}, fmt.Errorf("confidence: %w", err)
		}
		confidence = c
	}
	if raw, ok := changes["frp"]; ok {
		if err := json.Unmarshal(raw, &frp); err != nil {
			return Detection{}, fmt.Errorf("frp: %w", err)
		}
	}
	return NewDetection(base.Satellite, base.AcqDate, base.AcqTime, base.Latitude, base.Longitude, confidence, frp)
}
