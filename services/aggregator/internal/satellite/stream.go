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
}

// NewClient returns a client with streaming-safe defaults (no overall request timeout).
func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		Token:      token,
		HTTP:       &http.Client{},
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
	q := url.Values{"from": {strconv.FormatInt(*lastApplied, 10)}}
	req, err := c.newRequest(ctx, "/v1/stream", q)
	if err != nil {
		return false, handlerError{err}
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", strconv.FormatInt(*lastApplied, 10))
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
		if ev.Type == EventHeartbeat || ev.Offset <= *lastApplied {
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
		if typ != EventHeartbeat {
			off, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				return nil
			}
			ev.Offset = off
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

// ApplyEvent applies a feed event to the engine.
//
// Assumption, to verify against the feed: retract payloads carry either a "detection_key"
// (or "key") field or the full detection fields from which the key is derived.
func ApplyEvent(e *Engine, ev Event) (Disposition, []Change, error) {
	switch ev.Type {
	case EventDetection:
		d, err := ParseDetection(ev.Data)
		if err != nil {
			return "", nil, err
		}
		disp, changes := e.Add(d)
		return disp, changes, nil
	case EventUpdate:
		d, err := ParseDetection(ev.Data)
		if err != nil {
			return "", nil, err
		}
		disp, changes := e.Update(d)
		return disp, changes, nil
	case EventRetract:
		var ref struct {
			DetectionKey string `json:"detection_key"`
			Key          string `json:"key"`
		}
		if err := json.Unmarshal(ev.Data, &ref); err != nil {
			return "", nil, fmt.Errorf("decode retract: %w", err)
		}
		key := ref.DetectionKey
		if key == "" {
			key = ref.Key
		}
		if key == "" {
			d, err := ParseDetection(ev.Data)
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
