package adapter

import (
	"bytes"
	"strings"
	"testing"
)

const validReport = `{"report_id":"pvmbg-rpt-1","volcano_id":"VLC-001","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":750,"reported_at":"2026-09-05T15:30:00Z"}`

// Payloads produced by each mock-pvmbg schema mode (contracts/pvmbg.yaml).
var modePayloads = map[string]string{
	"rename_field":           `{"event_id":"pvmbg-rpt-2","volcano_id":"VLC-001","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":750,"reported_at":"2026-09-05T15:31:00Z"}`,
	"type_change":            `{"report_id":"pvmbg-rpt-3","volcano_id":"VLC-001","alert_level":"Siaga","eruption_count_24h":"12","ash_column_height_m":750,"reported_at":"2026-09-05T15:32:00Z"}`,
	"missing_required_field": `{"report_id":"pvmbg-rpt-4","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":750,"reported_at":"2026-09-05T15:33:00Z"}`,
}

func TestParseVolcanicBatchIsolatesEachIncompatibleMode(t *testing.T) {
	wantReason := map[string]string{
		"rename_field":           "schema_incompatible: missing required field 'report_id'",
		"type_change":            "schema_incompatible: field 'eruption_count_24h' changed from integer to string",
		"missing_required_field": "schema_incompatible: missing required field 'volcano_id'",
	}
	for mode, payload := range modePayloads {
		t.Run(mode, func(t *testing.T) {
			body := []byte("[" + validReport + "," + payload + "]")
			batch := ParseVolcanicBatch(body, PVMBGSchemaPolicy{})
			if len(batch.Reports) != 1 || batch.Reports[0].ReportID != "pvmbg-rpt-1" {
				t.Fatalf("valid report not accepted: %+v", batch.Reports)
			}
			if len(batch.Rejected) != 1 {
				t.Fatalf("rejected = %+v, want 1", batch.Rejected)
			}
			r := batch.Rejected[0]
			if r.Reason != wantReason[mode] || r.Retryable {
				t.Fatalf("reason/retryable = %q/%v, want %q/false", r.Reason, r.Retryable, wantReason[mode])
			}
			if string(r.Raw) != payload || r.MessageID != PVMBGMessageID([]byte(payload)) {
				t.Fatalf("raw payload not preserved: %s", r.Raw)
			}
		})
	}
}

func TestAdditiveFieldsStillAccepted(t *testing.T) {
	body := []byte(`[{"report_id":"r","volcano_id":"VLC-001","alert_level":"Siaga","eruption_count_24h":1,"ash_column_height_m":1,"reported_at":"2026-09-05T15:30:00Z","confidence_level":0.82}]`)
	batch := ParseVolcanicBatch(body, PVMBGSchemaPolicy{})
	if len(batch.Rejected) != 0 || len(batch.Reports) != 1 || batch.Reports[0].DynamicFields["confidence_level"] != 0.82 {
		t.Fatalf("batch = %+v", batch)
	}
}

func TestPolicyAcceptsRenameAndTypeChangeButNotMissingField(t *testing.T) {
	policy := PVMBGSchemaPolicy{FieldAliases: map[string]string{"event_id": "report_id"}, CoerceNumericStrings: true}
	r, err := DecodeVolcanicReport([]byte(modePayloads["rename_field"]), policy)
	if err != nil || r.ReportID != "pvmbg-rpt-2" || r.DynamicFields["event_id"] != nil {
		t.Fatalf("rename with alias = %+v, %v", r, err)
	}
	r, err = DecodeVolcanicReport([]byte(modePayloads["type_change"]), policy)
	if err != nil || r.EruptionCount24h != 12 {
		t.Fatalf("type_change with coercion = %+v, %v", r, err)
	}
	if _, err := DecodeVolcanicReport([]byte(modePayloads["missing_required_field"]), policy); err == nil {
		t.Fatal("missing volcano_id must stay rejected")
	}
	if _, err := DecodeVolcanicReport([]byte(`{"report_id":"r","volcano_id":"V","alert_level":"A","eruption_count_24h":"1.5","ash_column_height_m":1,"reported_at":"2026-09-05T15:30:00Z"}`), policy); err == nil {
		t.Fatal("non-integer eruption count must be rejected even with coercion")
	}
}

func TestPoisonInputsDoNotBlockValidReports(t *testing.T) {
	invalidUTF8 := []byte(`{"report_id":"bad-` + "\xff\xfe" + `","volcano_id":"VLC-001","alert_level":"Siaga","eruption_count_24h":1,"ash_column_height_m":1,"reported_at":"2026-09-05T15:30:00Z"}`)
	deep := []byte(`{"report_id":"deep","nested":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}`)
	body := bytes.Join([][]byte{[]byte("["), []byte(validReport), []byte(","), invalidUTF8, []byte(","), deep, []byte("]")}, nil)

	batch := ParseVolcanicBatch(body, PVMBGSchemaPolicy{})
	if len(batch.Reports) != 1 {
		t.Fatalf("valid report blocked: %+v", batch.Reports)
	}
	if len(batch.Rejected) != 2 {
		t.Fatalf("rejected = %d, want 2", len(batch.Rejected))
	}
	if !bytes.Equal(batch.Rejected[0].Raw, invalidUTF8) || !strings.HasPrefix(batch.Rejected[0].Reason, ReasonInvalidUTF8) {
		t.Fatalf("invalid UTF-8 not preserved byte-for-byte: %q / %s", batch.Rejected[0].Raw, batch.Rejected[0].Reason)
	}
	if !strings.HasPrefix(batch.Rejected[1].Reason, ReasonMalformedJSON) || !bytes.Equal(batch.Rejected[1].Raw, deep) {
		t.Fatalf("deep nesting reason = %s", batch.Rejected[1].Reason)
	}
}

func TestWholeBodyFailures(t *testing.T) {
	truncated := []byte(`[` + validReport + `,{"report_id":"cut`)
	batch := ParseVolcanicBatch(truncated, PVMBGSchemaPolicy{})
	if len(batch.Reports) != 0 || len(batch.Rejected) != 1 || !batch.Rejected[0].Retryable || !bytes.Equal(batch.Rejected[0].Raw, truncated) {
		t.Fatalf("truncated body = %+v", batch)
	}

	oversized := append([]byte("["), bytes.Repeat([]byte(" "), MaxPVMBGResponseBytes+10)...)
	batch = ParseVolcanicBatch(oversized, PVMBGSchemaPolicy{})
	r := batch.Rejected
	if len(r) != 1 || r[0].Retryable || len(r[0].Raw) != MaxPVMBGResponseBytes || !strings.HasPrefix(r[0].Reason, ReasonOversized) {
		t.Fatalf("oversized body: retryable=%v len=%d reason=%q", r[0].Retryable, len(r[0].Raw), r[0].Reason)
	}

	for _, body := range []string{`[]`, ` [ ] `} {
		if b := ParseVolcanicBatch([]byte(body), PVMBGSchemaPolicy{}); len(b.Reports)+len(b.Rejected) != 0 {
			t.Fatalf("empty array %q = %+v", body, b)
		}
	}
	for _, body := range []string{`{}`, `[1,]`, `[1]x`, `[,1]`} {
		if b := ParseVolcanicBatch([]byte(body), PVMBGSchemaPolicy{}); len(b.Rejected) != 1 || !b.Rejected[0].Retryable {
			t.Fatalf("malformed body %q = %+v", body, b)
		}
	}
}

func TestParsePVMBGFieldAliases(t *testing.T) {
	got, err := ParsePVMBGFieldAliases(" event_id=report_id , a=b ")
	if err != nil || got["event_id"] != "report_id" || got["a"] != "b" {
		t.Fatalf("aliases = %v, %v", got, err)
	}
	if _, err := ParsePVMBGFieldAliases("broken"); err == nil {
		t.Fatal("invalid alias should fail")
	}
}
