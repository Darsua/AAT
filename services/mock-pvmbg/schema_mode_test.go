package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newSchemaTestHandler(t *testing.T) (*service, http.Handler) {
	t.Helper()
	data, err := loadSeedData()
	if err != nil {
		t.Fatalf("loadSeedData() error = %v", err)
	}
	svc := newService(data)
	svc.delay = func() {}
	return svc, svc.routes("test-pvmbg-token")
}

func postSchemaVersion(t *testing.T, handler http.Handler, body string) (int, map[string]any) {
	t.Helper()
	request := authorizedRequest(http.MethodPost, "/admin/schema-version", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var response map[string]any
	_ = json.NewDecoder(recorder.Body).Decode(&response)
	return recorder.Code, response
}

func fetchRawReports(t *testing.T, handler http.Handler) []map[string]json.RawMessage {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authorizedRequest(http.MethodGet, "/volcanic-reports", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("reports status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var reports []map[string]json.RawMessage
	if err := json.NewDecoder(recorder.Body).Decode(&reports); err != nil {
		t.Fatalf("decode reports: %v", err)
	}
	if len(reports) == 0 {
		t.Fatal("expected at least one report")
	}
	return reports
}

func TestSchemaModesRewriteEveryReport(t *testing.T) {
	cases := []struct {
		mode  string
		check func(t *testing.T, stored VolcanicReport, served map[string]json.RawMessage)
	}{
		{"rename_field", func(t *testing.T, stored VolcanicReport, served map[string]json.RawMessage) {
			if _, ok := served["report_id"]; ok {
				t.Fatalf("report_id should be absent: %s", served["report_id"])
			}
			var eventID string
			if err := json.Unmarshal(served["event_id"], &eventID); err != nil || eventID != stored.ReportID {
				t.Fatalf("event_id = %s, want %q", served["event_id"], stored.ReportID)
			}
		}},
		{"type_change", func(t *testing.T, stored VolcanicReport, served map[string]json.RawMessage) {
			var count string
			if err := json.Unmarshal(served["eruption_count_24h"], &count); err != nil {
				t.Fatalf("eruption_count_24h should be a string, got %s", served["eruption_count_24h"])
			}
			var want int
			if err := json.Unmarshal([]byte(count), &want); err != nil || want != stored.EruptionCount24h {
				t.Fatalf("eruption_count_24h = %q, want decimal %d", count, stored.EruptionCount24h)
			}
		}},
		{"missing_required_field", func(t *testing.T, _ VolcanicReport, served map[string]json.RawMessage) {
			if _, ok := served["volcano_id"]; ok {
				t.Fatalf("volcano_id should be absent: %s", served["volcano_id"])
			}
			if _, ok := served["report_id"]; !ok {
				t.Fatal("report_id should remain present")
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			svc, handler := newSchemaTestHandler(t)
			code, response := postSchemaVersion(t, handler, `{"mode":"`+tc.mode+`"}`)
			if code != http.StatusOK || response["mode"] != tc.mode {
				t.Fatalf("schema-version status = %d, response = %v", code, response)
			}
			served := fetchRawReports(t, handler)
			stored := filterReports(svc.reports, nil)
			if len(served) != len(stored) {
				t.Fatalf("served %d reports, want %d", len(served), len(stored))
			}
			for i := range served {
				tc.check(t, stored[i], served[i])
			}
		})
	}
}

func TestSchemaModeLegacyRequestSelectsAdditive(t *testing.T) {
	_, handler := newSchemaTestHandler(t)
	if code, _ := postSchemaVersion(t, handler, `{"mode":"rename_field"}`); code != http.StatusOK {
		t.Fatalf("rename_field status = %d", code)
	}
	code, response := postSchemaVersion(t, handler, `{"include_confidence_level":true}`)
	if code != http.StatusOK || response["mode"] != "additive" || response["include_confidence_level"] != true {
		t.Fatalf("legacy request status = %d, response = %v", code, response)
	}
	for _, report := range fetchRawReports(t, handler) {
		if _, ok := report["report_id"]; !ok {
			t.Fatal("report_id should be restored in additive mode")
		}
		if _, ok := report["confidence_level"]; !ok {
			t.Fatal("confidence_level should be present after legacy toggle")
		}
	}
}

func TestSchemaModeCombinesWithConfidenceLevel(t *testing.T) {
	_, handler := newSchemaTestHandler(t)
	if code, _ := postSchemaVersion(t, handler, `{"include_confidence_level":true}`); code != http.StatusOK {
		t.Fatalf("confidence toggle status = %d", code)
	}
	// Omitting include_confidence_level keeps the current setting.
	code, response := postSchemaVersion(t, handler, `{"mode":"missing_required_field"}`)
	if code != http.StatusOK || response["include_confidence_level"] != true {
		t.Fatalf("mode-only request status = %d, response = %v", code, response)
	}
	for _, report := range fetchRawReports(t, handler) {
		if _, ok := report["confidence_level"]; !ok {
			t.Fatal("confidence_level should still be present")
		}
		if _, ok := report["volcano_id"]; ok {
			t.Fatal("volcano_id should be absent")
		}
	}
}

func TestSchemaVersionRejectsInvalidRequests(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"mode":"drop_everything"}`,
		`{"include_confidence_level":"yes"}`,
		`{"mode":7}`,
		`not json`,
	} {
		t.Run(body, func(t *testing.T) {
			svc, handler := newSchemaTestHandler(t)
			if code, _ := postSchemaVersion(t, handler, body); code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", code, http.StatusBadRequest)
			}
			if svc.mode != schemaModeAdditive || svc.includeConfidenceLevel {
				t.Fatalf("invalid request changed state: mode=%s confidence=%t", svc.mode, svc.includeConfidenceLevel)
			}
		})
	}
}

func TestOutageTakesPriorityOverSchemaMode(t *testing.T) {
	_, handler := newSchemaTestHandler(t)
	if code, _ := postSchemaVersion(t, handler, `{"mode":"type_change"}`); code != http.StatusOK {
		t.Fatalf("type_change status = %d", code)
	}
	request := authorizedRequest(http.MethodPost, "/admin/outage", bytes.NewBufferString(`{"is_outage":true}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("outage status = %d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, authorizedRequest(http.MethodGet, "/volcanic-reports", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("reports during outage = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}
