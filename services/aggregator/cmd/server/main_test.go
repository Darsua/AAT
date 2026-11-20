package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"bnpb/aggregator/internal/repository"
)

func TestRunSyncWithLeaseRenewal_RenewsDuringSlowSync(t *testing.T) {
	var renewals atomic.Int32
	var syncRan atomic.Bool

	lostLease, err := runSyncWithLeaseRenewal(
		context.Background(),
		func(context.Context) (bool, error) {
			renewals.Add(1)
			return true, nil
		},
		func(ctx context.Context) error {
			time.Sleep(85 * time.Millisecond)
			syncRan.Store(ctx.Err() == nil)
			return ctx.Err()
		},
		20*time.Millisecond,
		nil,
	)
	if err != nil {
		t.Fatalf("expected nil sync error, got %v", err)
	}
	if lostLease {
		t.Fatal("expected lostLease=false")
	}
	if !syncRan.Load() {
		t.Fatal("expected syncFn context to remain active")
	}
	if renewals.Load() < 2 {
		t.Fatalf("expected multiple background lease renewals, got %d", renewals.Load())
	}
}

func TestRunSyncWithLeaseRenewal_CancelsOnLostOrStaleLease(t *testing.T) {
	for _, tc := range []struct {
		name     string
		acquired bool
		renewErr error
	}{
		{name: "lease lost", acquired: false, renewErr: nil},
		{name: "stale fencing token", acquired: false, renewErr: repository.ErrStaleFencingToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lostLease, syncErr := runSyncWithLeaseRenewal(
				context.Background(),
				func(context.Context) (bool, error) {
					return tc.acquired, tc.renewErr
				},
				func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
				15*time.Millisecond,
				nil,
			)
			if !lostLease {
				t.Fatal("expected lostLease=true")
			}
			if !errors.Is(syncErr, context.Canceled) {
				t.Fatalf("expected context.Canceled from syncFn, got %v", syncErr)
			}
		})
	}
}
