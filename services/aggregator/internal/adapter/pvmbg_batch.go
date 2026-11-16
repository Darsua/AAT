package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"bnpb/aggregator/internal/logger"
)

// MaxPVMBGResponseBytes bounds how much of one /volcanic-reports response is read into memory.
const MaxPVMBGResponseBytes = 8 << 20

// Rejection reasons are prefixed so retry/park policy and audits can group them.
const (
	ReasonSchemaIncompatible = "schema_incompatible"
	ReasonInvalidUTF8        = "invalid_utf8"
	ReasonMalformedJSON      = "malformed_json"
	ReasonOversized          = "oversized_payload"
)

// PVMBGSchemaPolicy is the configurable part of the schema evolution policy. Additive
// fields are always accepted into attributes; incompatible changes are rejected unless a
// policy entry explicitly maps them back to the nominal schema.
type PVMBGSchemaPolicy struct {
	// FieldAliases maps an upstream field name to the nominal field it replaces,
	// e.g. {"event_id": "report_id"}. Used only when the nominal field is absent.
	FieldAliases map[string]string
	// CoerceNumericStrings accepts decimal strings for numeric nominal fields.
	CoerceNumericStrings bool
}

// ParsePVMBGFieldAliases parses "from=to,from2=to2" into an alias map.
func ParsePVMBGFieldAliases(raw string) (map[string]string, error) {
	aliases := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		from, to, ok := strings.Cut(entry, "=")
		from, to = strings.TrimSpace(from), strings.TrimSpace(to)
		if !ok || from == "" || to == "" {
			return nil, fmt.Errorf("invalid PVMBG field alias %q: expected from=to", entry)
		}
		aliases[from] = to
	}
	return aliases, nil
}

// RejectedPayload is one unprocessable input preserved byte-for-byte for the DLQ.
type RejectedPayload struct {
	MessageID   string // deterministic: sha256 of the raw bytes
	Raw         []byte
	Reason      string
	Retryable   bool
	SourceRefID string // best effort, for per-report replay ordering; may be empty
}

// PVMBGBatch is the outcome of one poll: accepted reports plus rejected inputs.
type PVMBGBatch struct {
	Reports  []VolcanicReportDTO
	Rejected []RejectedPayload
}

// PVMBGMessageID derives the DLQ message identity from raw bytes, so re-polling the same
// payload (overlap window) maps to the same DLQ row instead of creating duplicates.
func PVMBGMessageID(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "pvmbg-" + hex.EncodeToString(sum[:16])
}

func newRejection(raw []byte, reason string, retryable bool, sourceRefID string) RejectedPayload {
	return RejectedPayload{
		MessageID:   PVMBGMessageID(raw),
		Raw:         append([]byte(nil), raw...),
		Reason:      reason,
		Retryable:   retryable,
		SourceRefID: sourceRefID,
	}
}

// WithSchemaPolicy sets the schema policy used by FetchVolcanicBatch.
func (a *PVMBGAdapter) WithSchemaPolicy(policy PVMBGSchemaPolicy) *PVMBGAdapter {
	a.policy = policy
	return a
}

// SchemaPolicy returns the active schema policy.
func (a *PVMBGAdapter) SchemaPolicy() PVMBGSchemaPolicy { return a.policy }

// FetchVolcanicBatch polls /volcanic-reports and validates each report individually, so a
// poison record is isolated without blocking valid records in the same response.
// Transport failures and non-200 responses are returned as errors, not rejections.
func (a *PVMBGAdapter) FetchVolcanicBatch(ctx context.Context, since *time.Time) (PVMBGBatch, error) {
	reqURL := fmt.Sprintf("%s/volcanic-reports", a.baseURL)
	if since != nil && !since.IsZero() {
		q := url.Values{}
		q.Set("since", since.UTC().Format(time.RFC3339))
		reqURL += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return PVMBGBatch{}, fmt.Errorf("failed to create PVMBG request: %w", err)
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	resp, err := doLogged(ctx, a.httpClient, req, "pvmbg")
	if err != nil {
		return PVMBGBatch{}, fmt.Errorf("%w: failed to execute PVMBG request: %w", ErrUpstreamUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return PVMBGBatch{}, ErrUpstreamUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		return PVMBGBatch{}, fmt.Errorf("PVMBG endpoint returned non-200 status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxPVMBGResponseBytes+1))
	if err != nil {
		return PVMBGBatch{}, fmt.Errorf("%w: failed to read PVMBG response: %w", ErrUpstreamUnavailable, err)
	}
	return ParseVolcanicBatch(body, a.policy), nil
}

// ParseVolcanicBatch splits a response body into reports and rejected inputs.
//
// Whole-body failures (oversized, not a JSON array) are rejected as one message holding
// the bytes that were read. Malformed JSON is retryable, since a truncated transfer can
// succeed on the next poll; an oversized body is parked. Invalid UTF-8 is checked on the
// raw bytes because encoding/json would otherwise silently replace it.
func ParseVolcanicBatch(body []byte, policy PVMBGSchemaPolicy) PVMBGBatch {
	if len(body) > MaxPVMBGResponseBytes {
		reason := fmt.Sprintf("%s: response exceeded %d bytes; first %d bytes kept", ReasonOversized, MaxPVMBGResponseBytes, MaxPVMBGResponseBytes)
		return PVMBGBatch{Rejected: []RejectedPayload{newRejection(body[:MaxPVMBGResponseBytes], reason, false, "")}}
	}
	elements, err := splitTopLevelArray(body)
	if err != nil {
		reason := fmt.Sprintf("%s: response is not a JSON array of reports: %v", ReasonMalformedJSON, err)
		return PVMBGBatch{Rejected: []RejectedPayload{newRejection(body, reason, true, "")}}
	}

	var batch PVMBGBatch
	for _, raw := range elements {
		report, err := DecodeVolcanicReport(raw, policy)
		if err != nil {
			batch.Rejected = append(batch.Rejected, newRejection(raw, err.Error(), false, sourceRefHint(raw, policy)))
			continue
		}
		batch.Reports = append(batch.Reports, report)
	}
	return batch
}

// DecodeVolcanicReport validates one report against the nominal schema and the policy.
func DecodeVolcanicReport(raw []byte, policy PVMBGSchemaPolicy) (VolcanicReportDTO, error) {
	if !utf8.Valid(raw) {
		return VolcanicReportDTO{}, fmt.Errorf("%s: report contains invalid UTF-8", ReasonInvalidUTF8)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return VolcanicReportDTO{}, fmt.Errorf("%s: report is not a JSON object: %v", ReasonMalformedJSON, err)
	}
	for from, to := range policy.FieldAliases {
		if v, ok := fields[from]; ok {
			if _, nominal := fields[to]; !nominal {
				fields[to] = v
				delete(fields, from)
			}
		}
	}

	for _, name := range []string{"report_id", "volcano_id", "alert_level", "reported_at"} {
		if err := requireString(fields, name); err != nil {
			return VolcanicReportDTO{}, err
		}
	}
	if _, err := time.Parse(time.RFC3339, unquote(fields["reported_at"])); err != nil {
		return VolcanicReportDTO{}, fmt.Errorf("%s: field 'reported_at' is not an RFC3339 timestamp", ReasonSchemaIncompatible)
	}
	if err := requireNumber(fields, "eruption_count_24h", true, policy.CoerceNumericStrings); err != nil {
		return VolcanicReportDTO{}, err
	}
	if err := requireNumber(fields, "ash_column_height_m", false, policy.CoerceNumericStrings); err != nil {
		return VolcanicReportDTO{}, err
	}

	normalized, err := json.Marshal(fields)
	if err != nil {
		return VolcanicReportDTO{}, fmt.Errorf("%s: %v", ReasonMalformedJSON, err)
	}
	var report VolcanicReportDTO
	if err := json.Unmarshal(normalized, &report); err != nil {
		return VolcanicReportDTO{}, fmt.Errorf("%s: %v", ReasonSchemaIncompatible, err)
	}
	return report, nil
}

func jsonKind(v json.RawMessage) string {
	switch t := bytes.TrimSpace(v); {
	case len(t) == 0:
		return "empty"
	case t[0] == '"':
		return "string"
	case t[0] == '{':
		return "object"
	case t[0] == '[':
		return "array"
	case bytes.Equal(t, []byte("null")):
		return "null"
	case bytes.Equal(t, []byte("true")), bytes.Equal(t, []byte("false")):
		return "boolean"
	}
	return "number"
}

func unquote(v json.RawMessage) string {
	var s string
	_ = json.Unmarshal(v, &s)
	return s
}

func requireString(fields map[string]json.RawMessage, name string) error {
	v, ok := fields[name]
	if !ok {
		return fmt.Errorf("%s: missing required field '%s'", ReasonSchemaIncompatible, name)
	}
	if kind := jsonKind(v); kind != "string" {
		return fmt.Errorf("%s: field '%s' changed from string to %s", ReasonSchemaIncompatible, name, kind)
	}
	if strings.TrimSpace(unquote(v)) == "" {
		return fmt.Errorf("%s: required field '%s' is empty", ReasonSchemaIncompatible, name)
	}
	return nil
}

// requireNumber checks a numeric field, optionally coercing a decimal string in place.
func requireNumber(fields map[string]json.RawMessage, name string, integer, coerce bool) error {
	v, ok := fields[name]
	if !ok {
		return fmt.Errorf("%s: missing required field '%s'", ReasonSchemaIncompatible, name)
	}
	want := "number"
	if integer {
		want = "integer"
	}
	kind := jsonKind(v)
	if kind == "string" && coerce {
		s := strings.TrimSpace(unquote(v))
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			v, kind = json.RawMessage(s), "number"
			fields[name] = v
		}
	}
	if kind != "number" {
		return fmt.Errorf("%s: field '%s' changed from %s to %s", ReasonSchemaIncompatible, name, want, kind)
	}
	if integer {
		if _, err := strconv.ParseInt(string(bytes.TrimSpace(v)), 10, 64); err != nil {
			return fmt.Errorf("%s: field '%s' changed from integer to non-integer number", ReasonSchemaIncompatible, name)
		}
	}
	return nil
}

// sourceRefHint extracts a report identifier for ordering, trying aliases of report_id.
func sourceRefHint(raw []byte, policy PVMBGSchemaPolicy) string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return ""
	}
	if v, ok := fields["report_id"]; ok && jsonKind(v) == "string" {
		return unquote(v)
	}
	for from, to := range policy.FieldAliases {
		if to == "report_id" {
			if v, ok := fields[from]; ok && jsonKind(v) == "string" {
				return unquote(v)
			}
		}
	}
	return ""
}

// splitTopLevelArray returns the raw bytes of each element of a top-level JSON array.
// It tracks only brackets and strings, so an element nested deeper than encoding/json
// allows is still isolated as its own rejected record instead of failing the batch.
func splitTopLevelArray(body []byte) ([][]byte, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) < 2 || trimmed[0] != '[' {
		return nil, fmt.Errorf("expected a JSON array")
	}
	var (
		elements [][]byte
		depth    int
		inString bool
		escaped  bool
		start    = 1
	)
	for i := 1; i < len(trimmed); i++ {
		c := trimmed[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}':
			depth--
		case ']':
			if depth == 0 {
				if i != len(trimmed)-1 {
					return nil, fmt.Errorf("unexpected data after array end")
				}
				if el := bytes.TrimSpace(trimmed[start:i]); len(el) > 0 {
					elements = append(elements, el)
				} else if len(elements) > 0 {
					return nil, fmt.Errorf("trailing comma in array")
				}
				return elements, nil
			}
			depth--
		case ',':
			if depth == 0 {
				el := bytes.TrimSpace(trimmed[start:i])
				if len(el) == 0 {
					return nil, fmt.Errorf("empty array element")
				}
				elements = append(elements, el)
				start = i + 1
			}
		}
		if depth < 0 {
			return nil, fmt.Errorf("unbalanced brackets")
		}
	}
	return nil, fmt.Errorf("unexpected end of JSON input")
}
