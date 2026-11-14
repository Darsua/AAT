package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/repository"
)

func buildAuditTestFixture(t *testing.T) (*AuditHandler, *repository.HazardRepository, map[string]*repository.MemoryUnitBackend) {
	t.Helper()

	backends := map[string]*repository.MemoryUnitBackend{
		"shard-1":    repository.NewMemoryUnitBackend(),
		"shard-1-r1": repository.NewMemoryUnitBackend(),
		"shard-2":    repository.NewMemoryUnitBackend(),
		"shard-2-r1": repository.NewMemoryUnitBackend(),
	}

	groups := []repository.ShardGroup{
		{
			ShardID: "shard-1",
			Units: []repository.StorageUnit{
				{Unit: "shard-1", Node: "aqua", Role: domain.StorageRolePrimary, Backend: backends["shard-1"]},
				{Unit: "shard-1-r1", Node: "ruby", Role: domain.StorageRoleReplica, Backend: backends["shard-1-r1"]},
			},
		},
		{
			ShardID: "shard-2",
			Units: []repository.StorageUnit{
				{Unit: "shard-2", Node: "ruby", Role: domain.StorageRolePrimary, Backend: backends["shard-2"]},
				{Unit: "shard-2-r1", Node: "aqua", Role: domain.StorageRoleReplica, Backend: backends["shard-2-r1"]},
			},
		},
	}

	repo, err := repository.NewShardedHazardRepository(groups)
	if err != nil {
		t.Fatalf("failed to create sharded repo: %v", err)
	}
	handler := NewAuditHandler(repo, "super-audit-secret", "ALL")
	return handler, repo, backends
}

func authRequest(method, url string) *http.Request {
	req := httptest.NewRequest(method, url, nil)
	req.Header.Set("Authorization", "Bearer super-audit-secret")
	return req
}

func TestAuditHandler_Authentication(t *testing.T) {
	handler, _, _ := buildAuditTestFixture(t)

	for _, tc := range []struct {
		name string
		auth string
	}{
		{"missing header", ""},
		{"wrong scheme", "Basic dXNlcjpwYXNz"},
		{"wrong token", "Bearer wrong-token"},
		{"empty bearer", "Bearer   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", rec.Code)
			}
			var errResp AuditErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed to parse AuditErrorResponse: %v", err)
			}
			if errResp.Error.Code != "unauthorized" {
				t.Errorf("expected code unauthorized, got %q", errResp.Error.Code)
			}
		})
	}

	// Server with unconfigured empty AUDIT_TOKEN must fail closed.
	closedHandler := NewAuditHandler(nil, "", "ALL")
	rec := httptest.NewRecorder()
	closedHandler.ServeHTTP(rec, authRequest(http.MethodGet, "/internal/audit/events"))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when server token is empty, got %d", rec.Code)
	}
}

func TestAuditHandler_EventsOrderingPaginationAndPartial(t *testing.T) {
	handler, repo, backends := buildAuditTestFixture(t)
	ctx := context.Background()

	t0 := time.Date(2026, 11, 12, 2, 51, 8, 0, time.UTC)
	events := []domain.HazardEvent{
		{
			HazardID:    "haz-b",
			Source:      domain.SourceBMKG,
			SourceRefID: "bmkg-2",
			HazardType:  domain.HazardTypeSeismic,
			Severity:    domain.SeverityNormal,
			AreaName:    "Aceh",
			OccurredAt:  t0.Add(-5 * time.Minute),
			IngestedAt:  t0, // Tie on ingested_at with haz-a
			Attributes:  map[string]any{"magnitude": 4.2},
		},
		{
			HazardID:    "haz-a",
			Source:      domain.SourceSatelit,
			SourceRefID: "fire-0a41c7",
			HazardType:  domain.HazardTypeWildfire,
			Severity:    domain.SeveritySiaga,
			AreaName:    "Kalimantan Tengah",
			OccurredAt:  t0.Add(-8 * time.Minute),
			IngestedAt:  t0,
			Attributes: map[string]any{
				"detection_keys": []string{"N20|2026-11-12|243|-2.51230|113.80440"},
			},
		},
		{
			HazardID:    "haz-c",
			Source:      domain.SourcePVMBG,
			SourceRefID: "pvmbg-1",
			HazardType:  domain.HazardTypeVolcanic,
			Severity:    domain.SeverityWaspada,
			AreaName:    "Gunung Merapi",
			OccurredAt:  t0.Add(1 * time.Minute),
			IngestedAt:  t0.Add(10 * time.Second),
			Attributes:  map[string]any{"volcano_id": "VLC-001"},
		},
	}
	for i := range events {
		if err := repo.SaveHazard(ctx, &events[i]); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}

	// Page 1: limit=2 -> must return haz-a then haz-b cleanly
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, authRequest(http.MethodGet, "/internal/audit/events?limit=2"))
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 on page 1, got %d: %s", rec1.Code, rec1.Body.String())
	}
	var p1 repository.AuditEventsResult
	if err := json.Unmarshal(rec1.Body.Bytes(), &p1); err != nil {
		t.Fatalf("decode p1: %v", err)
	}
	if len(p1.Items) != 2 || p1.Items[0].HazardID != "haz-a" || p1.Items[1].HazardID != "haz-b" {
		t.Fatalf("unexpected page 1 items: %+v", p1.Items)
	}
	if p1.NextCursor == nil {
		t.Fatal("expected non-nil next_cursor on page 1")
	}

	// Verify satellite detection_keys exact string without rounding
	keys, ok := p1.Items[0].Attributes["detection_keys"].([]any)
	if !ok || len(keys) != 1 || keys[0] != "N20|2026-11-12|243|-2.51230|113.80440" {
		t.Errorf("detection_keys altered or rounded: %+v", p1.Items[0].Attributes["detection_keys"])
	}

	// Page 2 using opaque cursor -> must return haz-c and next_cursor == nil
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, authRequest(http.MethodGet, "/internal/audit/events?limit=2&cursor="+*p1.NextCursor))
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on page 2, got %d", rec2.Code)
	}
	var p2 repository.AuditEventsResult
	_ = json.Unmarshal(rec2.Body.Bytes(), &p2)
	if len(p2.Items) != 1 || p2.Items[0].HazardID != "haz-c" || p2.NextCursor != nil {
		t.Fatalf("unexpected page 2 result: %+v", p2)
	}

	// Offset cursor compatibility from OpenAPI example ({"o": 2})
	recOff := httptest.NewRecorder()
	handler.ServeHTTP(recOff, authRequest(http.MethodGet, "/internal/audit/events?cursor=eyJvIjoyfQ"))
	var pOff repository.AuditEventsResult
	_ = json.Unmarshal(recOff.Body.Bytes(), &pOff)
	if len(pOff.Items) != 1 || pOff.Items[0].HazardID != "haz-c" {
		t.Errorf("offset cursor compatibility failed: %+v", pOff.Items)
	}

	// since filter (ingested_at >= boundary)
	recSince := httptest.NewRecorder()
	handler.ServeHTTP(recSince, authRequest(http.MethodGet, "/internal/audit/events?since=2026-11-12T02:51:18Z"))
	var pSince repository.AuditEventsResult
	_ = json.Unmarshal(recSince.Body.Bytes(), &pSince)
	if len(pSince.Items) != 1 || pSince.Items[0].HazardID != "haz-c" {
		t.Errorf("since filter failed: %+v", pSince.Items)
	}

	// Partial outage (take ruby offline: shard-1-r1 & shard-2) -> returns partial:true, unavailable_units
	backends["shard-1-r1"].SetHealthy(false)
	backends["shard-2"].SetHealthy(false)
	recPartial := httptest.NewRecorder()
	handler.ServeHTTP(recPartial, authRequest(http.MethodGet, "/internal/audit/events"))
	if recPartial.Code != http.StatusOK {
		t.Fatalf("expected 200 during partial outage, got %d", recPartial.Code)
	}
	var pPart repository.AuditEventsResult
	_ = json.Unmarshal(recPartial.Body.Bytes(), &pPart)
	if !pPart.Partial || len(pPart.UnavailableUnits) != 2 || len(pPart.Items) != 3 {
		t.Errorf("unexpected partial response: %+v", pPart)
	}

	// Complete outage across ALL storage units -> HTTP 503
	backends["shard-1"].SetHealthy(false)
	backends["shard-2-r1"].SetHealthy(false)
	recDown := httptest.NewRecorder()
	handler.ServeHTTP(recDown, authRequest(http.MethodGet, "/internal/audit/events"))
	if recDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when all storage units are down, got %d", recDown.Code)
	}
}

func TestAuditHandler_Checksum(t *testing.T) {
	handler, repo, _ := buildAuditTestFixture(t)
	ctx := context.Background()

	// 1. Empty dataset checksum = SHA256("")
	recEmpty := httptest.NewRecorder()
	handler.ServeHTTP(recEmpty, authRequest(http.MethodGet, "/internal/audit/checksum?source=SATELIT"))
	if recEmpty.Code != http.StatusOK {
		t.Fatalf("expected 200 on empty checksum, got %d", recEmpty.Code)
	}
	var resEmpty repository.AuditChecksumResult
	_ = json.Unmarshal(recEmpty.Body.Bytes(), &resEmpty)
	if resEmpty.Count != 0 || resEmpty.Checksum != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("unexpected empty checksum: %+v", resEmpty)
	}

	// 2. Multiple records -> raw-byte ascending sort, '\n' joined, no trailing newline, no double count
	refs := []string{"fire-z10", "fire-a02", "fire-B05"} // Raw ASCII byte order: "fire-B05" < "fire-a02" < "fire-z10"
	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	for i, ref := range refs {
		h := domain.HazardEvent{
			HazardID:    "haz-sat-" + ref,
			Source:      domain.SourceSatelit,
			SourceRefID: ref,
			HazardType:  domain.HazardTypeWildfire,
			Severity:    domain.SeverityWaspada,
			AreaName:    "Riau",
			OccurredAt:  now.Add(time.Duration(i) * time.Second),
			IngestedAt:  now.Add(time.Duration(i) * time.Second),
		}
		_ = repo.SaveHazard(ctx, &h)
	}
	// Also save 1 BMKG record to confirm omitted source includes all sources and source=SATELIT isolates
	_ = repo.SaveHazard(ctx, &domain.HazardEvent{
		HazardID:    "haz-bmkg-1",
		Source:      domain.SourceBMKG,
		SourceRefID: "eq-001",
		HazardType:  domain.HazardTypeSeismic,
		Severity:    domain.SeverityNormal,
		AreaName:    "Padang",
		OccurredAt:  now,
		IngestedAt:  now,
	})

	recSat := httptest.NewRecorder()
	handler.ServeHTTP(recSat, authRequest(http.MethodGet, "/internal/audit/checksum?source=SATELIT"))
	var resSat repository.AuditChecksumResult
	_ = json.Unmarshal(recSat.Body.Bytes(), &resSat)

	sorted := slices.Clone(refs)
	slices.Sort(sorted)
	raw := strings.Join(sorted, "\n") // strictly no trailing '\n'
	expectedBytes := sha256.Sum256([]byte(raw))
	expectedHex := hex.EncodeToString(expectedBytes[:])

	if resSat.Count != 3 || resSat.Checksum != expectedHex || resSat.Source != "SATELIT" || resSat.Algorithm != "sha256" {
		t.Fatalf("checksum mismatch: got=(count=%d, hex=%s, src=%s), want=(3, %s, SATELIT)",
			resSat.Count, resSat.Checksum, resSat.Source, expectedHex)
	}

	// 3. Omitted source filter includes all 4 canonical records across sources
	recAll := httptest.NewRecorder()
	handler.ServeHTTP(recAll, authRequest(http.MethodGet, "/internal/audit/checksum"))
	if recAll.Code != http.StatusOK {
		t.Fatalf("expected 200 when source omitted, got %d", recAll.Code)
	}
	var resAll repository.AuditChecksumResult
	_ = json.Unmarshal(recAll.Body.Bytes(), &resAll)
	if resAll.Count != 4 || resAll.Source != "ALL" {
		t.Errorf("expected count=4 source=ALL when source omitted, got count=%d source=%q", resAll.Count, resAll.Source)
	}
}

func TestAuditHandler_LocationAndAbsentVsUnreachable(t *testing.T) {
	handler, repo, backends := buildAuditTestFixture(t)
	ctx := context.Background()

	now := time.Now().UTC()
	h := domain.HazardEvent{
		HazardID:    "haz-loc-1",
		Source:      domain.SourceBMKG,
		SourceRefID: "ref-loc-1",
		HazardType:  domain.HazardTypeSeismic,
		Severity:    domain.SeverityNormal,
		AreaName:    "Bali",
		OccurredAt:  now,
		IngestedAt:  now,
		Attributes:  map[string]any{"magnitude": 4.0},
	}
	if err := repo.SaveHazard(ctx, &h); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// 1. Healthy lookup -> primary + replica locations, NO unavailable_units field in JSON
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authRequest(http.MethodGet, "/internal/audit/location?hazard_id=haz-loc-1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var rawMap map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &rawMap)
	if _, forbidden := rawMap["unavailable_units"]; forbidden {
		t.Error("AuditLocationResponse must NOT contain unavailable_units field")
	}
	var locRes repository.AuditLocationResult
	_ = json.Unmarshal(rec.Body.Bytes(), &locRes)
	if locRes.Partial || len(locRes.Locations) != 2 || locRes.Locations[0].Role != domain.StorageRolePrimary || locRes.Locations[1].Role != domain.StorageRoleReplica {
		t.Errorf("unexpected locations: %+v", locRes)
	}

	// 2. Non-existent hazard while owning shard is healthy -> 404
	rec404 := httptest.NewRecorder()
	handler.ServeHTTP(rec404, authRequest(http.MethodGet, "/internal/audit/location?hazard_id=haz-missing-xyz"))
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for absent hazard on healthy shards, got %d", rec404.Code)
	}

	// 3. Unreachable owning shard for non-existent hazard -> MUST return 503, NEVER false 404!
	ownerShard := repo.LocateShard("haz-missing-xyz")
	if ownerShard == "shard-1" {
		backends["shard-1"].SetHealthy(false)
		backends["shard-1-r1"].SetHealthy(false)
	} else {
		backends["shard-2"].SetHealthy(false)
		backends["shard-2-r1"].SetHealthy(false)
	}
	rec503 := httptest.NewRecorder()
	handler.ServeHTTP(rec503, authRequest(http.MethodGet, "/internal/audit/location?hazard_id=haz-missing-xyz"))
	if rec503.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when owning shard is completely unreachable (must not mistake for 404), got %d", rec503.Code)
	}
}

func TestAuditHandler_UnprocessedAndValidation(t *testing.T) {
	handler, repo, _ := buildAuditTestFixture(t)
	ctx := context.Background()

	t0 := time.Date(2026, 11, 12, 2, 40, 0, 0, time.UTC)
	msgs := []domain.UnprocessedMessage{
		{
			MessageID:  "msg-parked-1",
			Source:     domain.SourcePVMBG,
			ReceivedAt: t0,
			Reason:     "schema_incompatible: rename_field",
			Retryable:  false,
			Attempts:   3,
			Status:     domain.UnprocessedStatusParked,
			RawPayload: []byte(`{"event_id":"x"}`),
		},
		{
			MessageID:  "msg-retrying-2",
			Source:     domain.SourceSensorPantai,
			ReceivedAt: t0.Add(5 * time.Second),
			Reason:     "transient_timeout",
			Retryable:  true,
			Attempts:   2,
			Status:     domain.UnprocessedStatusRetrying,
		},
		{
			MessageID:  "msg-recovered-3",
			Source:     domain.SourcePVMBG,
			ReceivedAt: t0.Add(1 * time.Second),
			Reason:     "recovered",
			Retryable:  false,
			Attempts:   1,
			Status:     domain.UnprocessedStatusRecovered, // Must be excluded
		},
	}
	for _, m := range msgs {
		if err := repo.SaveUnprocessedMessage(ctx, m); err != nil {
			t.Fatalf("save dlq message failed: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, authRequest(http.MethodGet, "/internal/audit/unprocessed?limit=1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var p1 repository.AuditUnprocessedResult
	_ = json.Unmarshal(rec.Body.Bytes(), &p1)
	if len(p1.Items) != 1 || p1.Items[0].MessageID != "msg-parked-1" || p1.NextCursor == nil {
		t.Fatalf("unexpected unprocessed page 1: %+v", p1)
	}

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, authRequest(http.MethodGet, "/internal/audit/unprocessed?cursor="+*p1.NextCursor))
	var p2 repository.AuditUnprocessedResult
	_ = json.Unmarshal(rec2.Body.Bytes(), &p2)
	if len(p2.Items) != 1 || p2.Items[0].MessageID != "msg-retrying-2" || p2.NextCursor != nil {
		t.Fatalf("unexpected unprocessed page 2 (recovered must be excluded): %+v", p2)
	}

	// Validation errors
	for _, badURL := range []string{
		"/internal/audit/events?source=FIELD_REPORT",
		"/internal/audit/events?limit=1001",
		"/internal/audit/events?since=2026-11-12T02:00:00%2B07:00",
		"/internal/audit/events?cursor=not-valid-b64",
		"/internal/audit/checksum?source=UNKNOWN",
		"/internal/audit/location",
		"/internal/audit/unprocessed?limit=0",
	} {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, authRequest(http.MethodGet, badURL))
		if r.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for %s, got %d", badURL, r.Code)
		}
	}
}
