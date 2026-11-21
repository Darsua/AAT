package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultFieldReportsTopic = "bnpb.field-reports"
	maxFieldReportBodyBytes  = 1 << 20 // 1 MiB
	defaultIdempotencyTTL    = 24 * time.Hour
	defaultIdempotencyCap    = 10000
	defaultStageTimeout      = 2 * time.Second
)

// FieldReportRequest matches contracts/client_api.yaml FieldReportRequest.
type FieldReportRequest struct {
	ReportID    string         `json:"report_id,omitempty"`
	ReporterID  string         `json:"reporter_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude,omitempty"`
	Longitude   *float64       `json:"longitude,omitempty"`
	OccurredAt  string         `json:"occurred_at"`
	Description string         `json:"description,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
}

// FieldReportResponse matches contracts/client_api.yaml FieldReportResponse.
type FieldReportResponse struct {
	ReportID   string `json:"report_id"`
	Status     string `json:"status"`
	ReceivedAt string `json:"received_at"`
}

// StagedFieldReport is the durable envelope written to the broker staging topic.
type StagedFieldReport struct {
	ReportID       string         `json:"report_id"`
	ReporterID     string         `json:"reporter_id"`
	SubmittedBy    string         `json:"submitted_by"`
	HazardType     string         `json:"hazard_type"`
	Severity       string         `json:"severity"`
	AreaName       string         `json:"area_name"`
	Latitude       *float64       `json:"latitude,omitempty"`
	Longitude      *float64       `json:"longitude,omitempty"`
	OccurredAt     string         `json:"occurred_at"`
	Description    string         `json:"description,omitempty"`
	Details        map[string]any `json:"details,omitempty"`
	ReceivedAt     string         `json:"received_at"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	PayloadSHA256  string         `json:"payload_sha256"`
}

// FieldReportStager durably stages accepted field reports before returning 202.
type FieldReportStager interface {
	Stage(ctx context.Context, report StagedFieldReport) error
	Close() error
}

// KafkaFieldReportStager writes field reports to Redpanda/Kafka with RequireAll acks.
type KafkaFieldReportStager struct {
	writer *kafka.Writer
}

func NewKafkaFieldReportStager(brokers []string, topic string) *KafkaFieldReportStager {
	if topic == "" {
		topic = defaultFieldReportsTopic
	}
	return &KafkaFieldReportStager{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchSize:    1,
			BatchTimeout: time.Millisecond,
			WriteTimeout: defaultStageTimeout,
			MaxAttempts:  2,
		},
	}
}

func (s *KafkaFieldReportStager) Stage(ctx context.Context, report StagedFieldReport) error {
	payload, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("marshal staged field report: %w", err)
	}

	headers := []kafka.Header{}
	if report.CorrelationID != "" {
		headers = append(headers, kafka.Header{Key: correlationHead, Value: []byte(report.CorrelationID)})
	}
	if report.IdempotencyKey != "" {
		headers = append(headers, kafka.Header{Key: "Idempotency-Key", Value: []byte(report.IdempotencyKey)})
	}
	if st := traceFromContext(ctx); st.TraceID != "" && st.SpanID != "" {
		headers = append(headers, kafka.Header{
			Key:   traceparentHeader,
			Value: []byte(formatTraceparent(st.TraceID, st.SpanID, st.Sampled)),
		})
	}

	return s.writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(report.ReportID),
		Value:   payload,
		Headers: headers,
	})
}

func (s *KafkaFieldReportStager) Close() error {
	if s.writer == nil {
		return nil
	}
	return s.writer.Close()
}

type idempotencyEntry struct {
	payloadHash string
	done        chan struct{}
	response    FieldReportResponse
	err         error
	expiresAt   time.Time
}

type idempotencyStore struct {
	mu      sync.Mutex
	entries map[string]*idempotencyEntry
	order   []string
	ttl     time.Duration
	maxSize int
}

func newIdempotencyStore(ttl time.Duration, maxSize int) *idempotencyStore {
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	if maxSize <= 0 {
		maxSize = defaultIdempotencyCap
	}
	return &idempotencyStore{
		entries: make(map[string]*idempotencyEntry),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

func (s *idempotencyStore) acquire(key, payloadHash string, now time.Time) (existing *idempotencyEntry, leader *idempotencyEntry, conflict bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked(now)

	if entry, ok := s.entries[key]; ok {
		if entry.payloadHash != payloadHash {
			return nil, nil, true
		}
		return entry, nil, false
	}

	if len(s.entries) >= s.maxSize && len(s.order) > 0 {
		kept := s.order[:0]
		evicted := false
		for _, candidate := range s.order {
			e, exists := s.entries[candidate]
			if !exists {
				continue
			}
			if !evicted {
				select {
				case <-e.done:
					delete(s.entries, candidate)
					evicted = true
					continue
				default:
				}
			}
			kept = append(kept, candidate)
		}
		if len(kept) == 0 {
			s.order = nil
		} else {
			s.order = append([]string(nil), kept...)
		}
	}

	leader = &idempotencyEntry{
		payloadHash: payloadHash,
		done:        make(chan struct{}),
		expiresAt:   now.Add(s.ttl),
	}
	s.entries[key] = leader
	s.order = append(s.order, key)
	return nil, leader, false
}

func (s *idempotencyStore) complete(key string, leader *idempotencyEntry, resp FieldReportResponse, err error, now time.Time) {
	s.mu.Lock()
	if err != nil {
		if cur, ok := s.entries[key]; ok && cur == leader {
			delete(s.entries, key)
		}
		leader.err = err
	} else {
		leader.response = resp
		leader.expiresAt = now.Add(s.ttl)
	}
	close(leader.done)
	s.mu.Unlock()
}

func (s *idempotencyStore) pruneExpiredLocked(now time.Time) {
	if len(s.order) == 0 {
		return
	}
	kept := s.order[:0]
	for _, k := range s.order {
		e, ok := s.entries[k]
		if !ok {
			continue
		}
		select {
		case <-e.done:
			if now.After(e.expiresAt) {
				delete(s.entries, k)
				continue
			}
		default:
		}
		kept = append(kept, k)
	}
	s.order = kept
}

var (
	errIdempotencyConflict = errors.New("idempotency key reused with a different request payload")
	allowedFieldReportTypes = map[string]bool{
		"SEISMIC":  true,
		"VOLCANIC": true,
		"WILDFIRE": true,
		"COASTAL":  true,
	}
	allowedFieldReportSeverities = map[string]bool{
		"NORMAL":  true,
		"WASPADA": true,
		"SIAGA":   true,
		"AWAS":    true,
	}
)

func (h *apiHandler) submitFieldReport(w http.ResponseWriter, r *http.Request) {
	claims := claimsFromContext(r.Context())
	if claims == nil || claims.Scope != scopeAll {
		writeJSON(w, http.StatusForbidden, errorResponse{
			Error:   "FORBIDDEN",
			Message: "Scope 'hazards:read:all' required to submit field reports",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxFieldReportBodyBytes)
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error:   "BAD_REQUEST",
			Message: "Unable to read request body",
		})
		return
	}

	req, err := decodeAndValidateFieldReport(rawBody)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error:   "BAD_REQUEST",
			Message: err.Error(),
		})
		return
	}

	if h.reportStager == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{
			Error:   "SERVICE_UNAVAILABLE",
			Message: "Durable staging is not configured; report not accepted",
		})
		return
	}

	payloadHash := hashFieldReport(req)
	rawIdemHeader := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	idemKey := deriveIdempotencyKey(claims.Subject, rawIdemHeader, req.ReportID)

	if req.ReportID == "" {
		if rawIdemHeader != "" {
			req.ReportID = deterministicReportID(idemKey)
		} else {
			req.ReportID = generateReportID()
		}
	}

	var leader *idempotencyEntry
	if idemKey != "" && h.idempotency != nil {
		for {
			existing, newLeader, conflict := h.idempotency.acquire(idemKey, payloadHash, time.Now().UTC())
			if conflict {
				writeJSON(w, http.StatusConflict, errorResponse{
					Error:   "CONFLICT",
					Message: errIdempotencyConflict.Error(),
				})
				return
			}
			if newLeader != nil {
				leader = newLeader
				break
			}
			select {
			case <-existing.done:
				if existing.err == nil {
					writeJSON(w, http.StatusAccepted, existing.response)
					return
				}
			case <-r.Context().Done():
				writeJSON(w, http.StatusServiceUnavailable, errorResponse{
					Error:   "SERVICE_UNAVAILABLE",
					Message: "Request canceled while waiting for duplicate submission",
				})
				return
			}
		}
	}

	receivedAt := time.Now().UTC().Format(time.RFC3339Nano)
	corrID := correlationIDFromContext(r.Context())
	staged := StagedFieldReport{
		ReportID:       req.ReportID,
		ReporterID:     req.ReporterID,
		SubmittedBy:    claims.Subject,
		HazardType:     req.HazardType,
		Severity:       req.Severity,
		AreaName:       req.AreaName,
		Latitude:       req.Latitude,
		Longitude:      req.Longitude,
		OccurredAt:     req.OccurredAt,
		Description:    req.Description,
		Details:        req.Details,
		ReceivedAt:     receivedAt,
		CorrelationID:  corrID,
		IdempotencyKey: rawIdemHeader,
		PayloadSHA256:  payloadHash,
	}

	stageCtx, cancel := context.WithTimeout(r.Context(), defaultStageTimeout)
	defer cancel()

	started := time.Now()
	err = h.reportStager.Stage(stageCtx, staged)
	durMs := time.Since(started).Milliseconds()

	if err != nil {
		if leader != nil {
			h.idempotency.complete(idemKey, leader, FieldReportResponse{}, err, time.Now().UTC())
		}
		log.Printf("[client-api] field_report_stage_failed correlation_id=%q report_id=%q duration_ms=%d error=%q",
			corrID, req.ReportID, durMs, err)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{
			Error:   "SERVICE_UNAVAILABLE",
			Message: "Durable staging unavailable; report not accepted",
		})
		return
	}

	resp := FieldReportResponse{
		ReportID:   req.ReportID,
		Status:     "accepted",
		ReceivedAt: receivedAt,
	}
	if leader != nil {
		h.idempotency.complete(idemKey, leader, resp, nil, time.Now().UTC())
	}

	log.Printf("[client-api] field_report_accepted correlation_id=%q report_id=%q reporter_id=%q hazard_type=%q severity=%q duration_ms=%d",
		corrID, req.ReportID, req.ReporterID, req.HazardType, req.Severity, durMs)
	writeJSON(w, http.StatusAccepted, resp)
}

func decodeAndValidateFieldReport(raw []byte) (FieldReportRequest, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return FieldReportRequest{}, errors.New("Request body is required")
	}

	var req FieldReportRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return FieldReportRequest{}, fmt.Errorf("Invalid JSON payload: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return FieldReportRequest{}, errors.New("Request body must contain a single JSON object")
	}

	req.ReportID = strings.TrimSpace(req.ReportID)
	if len(req.ReportID) > 118 {
		return FieldReportRequest{}, errors.New("report_id must be 118 characters or fewer")
	}
	req.ReporterID = strings.TrimSpace(req.ReporterID)
	req.HazardType = strings.ToUpper(strings.TrimSpace(req.HazardType))
	req.Severity = strings.ToUpper(strings.TrimSpace(req.Severity))
	req.AreaName = strings.TrimSpace(req.AreaName)
	req.OccurredAt = strings.TrimSpace(req.OccurredAt)
	req.Description = strings.TrimSpace(req.Description)

	if req.ReporterID == "" {
		return FieldReportRequest{}, errors.New("reporter_id is required")
	}
	if !allowedFieldReportTypes[req.HazardType] {
		return FieldReportRequest{}, errors.New("Invalid hazard_type. Allowed: SEISMIC, VOLCANIC, WILDFIRE, COASTAL")
	}
	if !allowedFieldReportSeverities[req.Severity] {
		return FieldReportRequest{}, errors.New("Invalid severity. Allowed: NORMAL, WASPADA, SIAGA, AWAS")
	}
	if req.AreaName == "" {
		return FieldReportRequest{}, errors.New("area_name is required")
	}
	if req.OccurredAt == "" {
		return FieldReportRequest{}, errors.New("occurred_at is required")
	}
	parsedOccurred, err := time.Parse(time.RFC3339Nano, req.OccurredAt)
	if err != nil {
		return FieldReportRequest{}, errors.New("Invalid occurred_at format. Expected ISO 8601 UTC timestamp")
	}
	if parsedOccurred.UTC().After(time.Now().UTC().Add(5 * time.Minute)) {
		return FieldReportRequest{}, errors.New("occurred_at cannot be more than 5 minutes in the future")
	}
	req.OccurredAt = parsedOccurred.UTC().Format(time.RFC3339Nano)

	if req.Latitude != nil {
		if math.IsNaN(*req.Latitude) || math.IsInf(*req.Latitude, 0) || *req.Latitude < -90 || *req.Latitude > 90 {
			return FieldReportRequest{}, errors.New("latitude must be between -90 and 90")
		}
	}
	if req.Longitude != nil {
		if math.IsNaN(*req.Longitude) || math.IsInf(*req.Longitude, 0) || *req.Longitude < -180 || *req.Longitude > 180 {
			return FieldReportRequest{}, errors.New("longitude must be between -180 and 180")
		}
	}

	return req, nil
}

func hashFieldReport(req FieldReportRequest) string {
	normalized, _ := json.Marshal(req)
	sum := sha256.Sum256(normalized)
	return hex.EncodeToString(sum[:])
}

func deriveIdempotencyKey(subject, headerKey, reportID string) string {
	if headerKey != "" {
		return "idem:" + subject + ":" + headerKey
	}
	if reportID != "" {
		return "report:" + subject + ":" + reportID
	}
	return ""
}

func deterministicReportID(idemKey string) string {
	sum := sha256.Sum256([]byte(idemKey))
	return "rep-idem-" + hex.EncodeToString(sum[:8])
}

func generateReportID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("rep-fld-%d", time.Now().UnixNano())
	}
	return "rep-fld-" + hex.EncodeToString(b)
}
