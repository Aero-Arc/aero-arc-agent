package agent

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/google/uuid"
	"github.com/makinje/aero-arc-agent/internal/wal"
)

// One constant-size accumulator per unfinished target replaces the lossy frame
// queue. Only the worker accesses SQLite; the reader retains milestone facts and
// fresh sample pairs even while a write is blocked. Memory is independent of the
// MAVLink frame rate and is reclaimed after durable completion.
type completionTracker struct {
	watch    wal.FlightWatch
	samples  completionSamples
	epoch    string
	evidence *pb.FlightCompletionEvidence
	revision uint64
	saved    uint64
}

func (a *Agent) trackFlightCompletion(watch wal.FlightWatch) {
	if watch.Done || watch.HandoffAt == 0 || watch.Command.GetMavlink().GetMissionPrecondition() == nil {
		return
	}
	a.completionMu.Lock()
	defer a.completionMu.Unlock()
	if a.completionTrackers == nil {
		a.completionTrackers = make(map[string]*completionTracker)
	}
	if a.completionWake == nil {
		a.completionWake = make(chan struct{}, 1)
	}
	if old := a.completionTrackers[watch.Target]; old != nil && old.watch.Command.CommandId == watch.Command.CommandId {
		return
	}
	a.completionTrackers[watch.Target] = &completionTracker{watch: watch, epoch: uuid.NewString()}
}

// acceptCompletionStart opens the observation epoch before the applied journal
// write, so slow storage cannot lose post-ACK samples. The worker still requires
// that exact applied boundary to be durable before persisting any milestones.
func (a *Agent) acceptCompletionStart(commandID string, appliedAfter int64) {
	a.completionMu.Lock()
	defer a.completionMu.Unlock()
	for _, t := range a.completionTrackers {
		if t.watch.Command.CommandId == commandID && t.watch.AppliedAfter == 0 {
			t.watch.AppliedAfter = appliedAfter
			t.samples = completionSamples{}
		}
	}
}

func (a *Agent) restoreCompletionTrackers(ctx context.Context) error {
	a.completionMu.Lock()
	if a.completionWake == nil {
		a.completionWake = make(chan struct{}, 1)
	}
	a.completionMu.Unlock()
	if a.wal == nil {
		return nil
	}
	targets, err := a.wal.UnresolvedFlightWatchTargets(ctx)
	if err != nil {
		return err
	}
	for _, target := range targets {
		watch, err := a.wal.LoadUnresolvedFlightWatch(ctx, target)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Error("flight completion tracking unavailable", "target", target, "error", err)
			}
			continue
		}
		a.trackFlightCompletion(watch)
	}
	return nil
}

func (a *Agent) accumulateCompletion(o completionObservation) {
	a.completionMu.Lock()
	defer a.completionMu.Unlock()
	t := a.completionTrackers[o.target]
	if t == nil {
		return
	}
	changed, evidence, err := reduceCompletion(&t.watch, o, t.epoch, &t.samples)
	if err != nil {
		slog.Error("flight completion reduction failed", "error", err)
		return
	}
	if !changed {
		return
	}
	t.revision++
	if evidence != nil {
		t.evidence = evidence
	}
	// Wakeups may coalesce; the facts above cannot be dropped by a full channel.
	select {
	case a.completionWake <- struct{}{}:
	default:
	}
}

func (a *Agent) flushCompletionTrackers(ctx context.Context) {
	if a.wal == nil {
		return
	}
	type pending struct {
		tracker  *completionTracker
		watch    wal.FlightWatch
		evidence *pb.FlightCompletionEvidence
		revision uint64
	}
	a.completionMu.Lock()
	var writes []pending
	for _, t := range a.completionTrackers {
		if t.revision != t.saved {
			writes = append(writes, pending{t, t.watch, t.evidence, t.revision})
		}
	}
	a.completionMu.Unlock()
	for _, p := range writes {
		// Accumulation can begin before COMMAND_ACK arrives. Publication still
		// requires persisted applied start authority, including after a retry.
		owner, err := a.wal.LoadUnresolvedFlightWatch(ctx, p.watch.Target)
		if err == nil && (owner.Command.CommandId != p.watch.Command.CommandId || owner.HandoffAt != p.watch.HandoffAt || owner.AppliedAfter != p.watch.AppliedAfter) {
			err = errors.New("completion authority changed")
		}
		if err == nil {
			err = a.wal.SaveFlightWatch(ctx, p.watch, p.evidence)
		}
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, sql.ErrNoRows) {
				slog.Error("flight completion evidence persistence failed; retaining milestones", "error", err)
			}
			continue
		}
		a.completionMu.Lock()
		if current := a.completionTrackers[p.watch.Target]; current == p.tracker {
			current.saved = p.revision
			if p.watch.Done {
				delete(a.completionTrackers, p.watch.Target)
			}
		}
		a.completionMu.Unlock()
	}
}

func (a *Agent) completionWritesPending() bool {
	a.completionMu.Lock()
	defer a.completionMu.Unlock()
	for _, t := range a.completionTrackers {
		if t.revision != t.saved {
			return true
		}
	}
	return false
}
