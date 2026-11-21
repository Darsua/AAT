package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"bnpb/aggregator/internal/adapter"
	"bnpb/aggregator/internal/domain"
	"bnpb/aggregator/internal/repository"
)

const (
	defaultDLQRetryBudget = 5
	dlqPageSize           = 1000
	replayInterval        = 30 * time.Second
	replayBatchLimit      = 200
)

// errRetryableRejection keeps the cursor in place while a retryable rejection still has budget.
var errRetryableRejection = errors.New("retryable PVMBG payload rejection pending")

// UnprocessedStore is the durable DLQ owned by the Aggregator (dlq_messages, #26/#30).
type UnprocessedStore interface {
	SaveUnprocessedMessage(ctx context.Context, msg domain.UnprocessedMessage) error
	QueryAuditUnprocessed(ctx context.Context, filter repository.AuditUnprocessedFilter) (repository.AuditUnprocessedResult, error)
}

type pvmbgDLQ struct {
	retryBudget int
	lastReplay  time.Time
	now         func() time.Time
}

// WithDLQRetryBudget sets how many times a retryable rejection is retried before parking.
func (p *PVMBGPoller) WithDLQRetryBudget(budget int) *PVMBGPoller {
	if budget > 0 {
		p.dlq.retryBudget = budget
	}
	return p
}

func (p *PVMBGPoller) dlqStore() (UnprocessedStore, bool) {
	s, ok := p.repo.(UnprocessedStore)
	return s, ok
}

// activeDLQ lists retrying/parked PVMBG messages ordered by (received_at, message_id).
func (p *PVMBGPoller) activeDLQ(ctx context.Context, store UnprocessedStore) ([]domain.UnprocessedMessage, error) {
	source := domain.SourcePVMBG
	var (
		all    []domain.UnprocessedMessage
		cursor string
	)
	for {
		page, err := store.QueryAuditUnprocessed(ctx, repository.AuditUnprocessedFilter{Source: &source, Limit: dlqPageSize, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.NextCursor == nil {
			return all, nil
		}
		cursor = *page.NextCursor
	}
}

// recordRejections persists every rejected payload before the cursor can advance.
// Incompatible payloads are parked once and not rewritten on later polls, so they never
// hot-loop; retryable ones count attempts against the budget and then park.
// It returns errRetryableRejection while any retryable payload is still retrying.
func (p *PVMBGPoller) recordRejections(ctx context.Context, rejected []adapter.RejectedPayload) error {
	if len(rejected) == 0 {
		return nil
	}
	store, ok := p.dlqStore()
	if !ok {
		return fmt.Errorf("%d PVMBG payloads rejected but no DLQ store is configured", len(rejected))
	}
	active, err := p.activeDLQ(ctx, store)
	if err != nil {
		return fmt.Errorf("load PVMBG DLQ state: %w", err)
	}
	existing := make(map[string]domain.UnprocessedMessage, len(active))
	for _, m := range active {
		existing[m.MessageID] = m
	}

	pending := false
	for _, r := range rejected {
		prev, seen := existing[r.MessageID]
		if seen && !r.Retryable {
			continue
		}
		msg := domain.UnprocessedMessage{
			MessageID:  r.MessageID,
			Source:     domain.SourcePVMBG,
			ReceivedAt: p.dlq.now().UTC(),
			Reason:     r.Reason,
			Retryable:  r.Retryable,
			Attempts:   1,
			Status:     domain.UnprocessedStatusParked,
			RawPayload: r.Raw,
		}
		if seen {
			msg.ReceivedAt = prev.ReceivedAt
			msg.Attempts = prev.Attempts + 1
		}
		if r.Retryable {
			if seen && prev.Status == domain.UnprocessedStatusParked {
				continue // budget already spent; wait for a system change and replay
			}
			if msg.Attempts < p.dlq.retryBudget {
				msg.Status = domain.UnprocessedStatusRetrying
				pending = true
			}
		}
		if err := store.SaveUnprocessedMessage(ctx, msg); err != nil {
			return fmt.Errorf("persist rejected PVMBG payload %s: %w", msg.MessageID, err)
		}
		p.log.Warn("pvmbg_payload_rejected",
			slog.String("message_id", msg.MessageID),
			slog.String("status", string(msg.Status)),
			slog.Int("attempts", msg.Attempts),
			slog.String("reason", msg.Reason),
		)
	}
	if pending {
		return errRetryableRejection
	}
	return nil
}

// replayParked re-validates stored rejections with the current schema policy and applies
// the ones that now decode, oldest first, then marks them recovered so they leave the
// audit view. A report older than the stored hazard for the same report_id is marked
// recovered without being written, so replay never rolls a hazard back.
func (p *PVMBGPoller) replayParked(ctx context.Context, fence *domain.WriteFence) error {
	store, ok := p.dlqStore()
	if !ok || p.dlq.now().Sub(p.dlq.lastReplay) < replayInterval {
		return nil
	}
	active, err := p.activeDLQ(ctx, store)
	if err != nil {
		return fmt.Errorf("load PVMBG DLQ for replay: %w", err)
	}
	p.dlq.lastReplay = p.dlq.now()

	recovered := 0
	for _, msg := range active {
		if recovered >= replayBatchLimit {
			break
		}
		reports, ok := p.decodeStored(msg.RawPayload)
		if !ok {
			continue
		}
		for _, report := range reports {
			if err := p.applyReplayed(ctx, report, fence); err != nil {
				return err
			}
		}
		msg.Status = domain.UnprocessedStatusRecovered
		if err := store.SaveUnprocessedMessage(ctx, msg); err != nil {
			return fmt.Errorf("mark PVMBG message %s recovered: %w", msg.MessageID, err)
		}
		recovered++
	}
	if recovered > 0 {
		p.log.Info("pvmbg_dlq_replayed", slog.Int("recovered", recovered), slog.Int("scanned", len(active)))
	}
	return nil
}

// decodeStored decodes a stored payload: a single report, or a whole response body that
// must now decode without any rejection.
func (p *PVMBGPoller) decodeStored(raw []byte) ([]adapter.VolcanicReportDTO, bool) {
	policy := p.adapter.SchemaPolicy()
	if t := bytes.TrimSpace(raw); len(t) > 0 && t[0] == '[' {
		batch := adapter.ParseVolcanicBatch(raw, policy)
		return batch.Reports, len(batch.Rejected) == 0
	}
	report, err := adapter.DecodeVolcanicReport(raw, policy)
	if err != nil {
		return nil, false
	}
	return []adapter.VolcanicReportDTO{report}, true
}

func (p *PVMBGPoller) applyReplayed(ctx context.Context, report adapter.VolcanicReportDTO, fence *domain.WriteFence) error {
	canonical, err := p.normalizer.NormalizeVolcanic(ctx, report)
	if err != nil {
		return fmt.Errorf("normalize replayed report %s: %w", report.ReportID, err)
	}
	current, err := p.repo.GetHazardBySourceRef(ctx, domain.SourcePVMBG, canonical.SourceRefID)
	if err != nil {
		return fmt.Errorf("look up hazard for replayed report %s: %w", report.ReportID, err)
	}
	if current != nil && current.OccurredAt.After(canonical.OccurredAt) {
		return nil
	}
	corrID := fmt.Sprintf("pvmbg-replay-%s", report.ReportID)
	if err := p.lease.saveHazard(ctx, &canonical, corrID, fence); err != nil {
		return fmt.Errorf("persist replayed report %s: %w", report.ReportID, err)
	}
	return nil
}
