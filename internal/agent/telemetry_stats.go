package agent

import (
	"context"
	"log/slog"
	"time"
)

func (a *Agent) runTelemetryStats(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastIngest := uint64(0)
	lastSent := uint64(0)
	last := time.Now()

	for {
		select {
		case <-ticker.C:
			if a.wal == nil {
				continue
			}
			now := time.Now()
			elapsed := now.Sub(last).Seconds()
			if elapsed <= 0 {
				last = now
				continue
			}

			ingestTotal := a.ingestCount.Load()
			sentTotal := a.sendCount.Load()

			ingestRate := float64(ingestTotal-lastIngest) / elapsed
			sendRate := float64(sentTotal-lastSent) / elapsed

			lastIngest = ingestTotal
			lastSent = sentTotal
			last = now

			undelivered, err := a.wal.CountUndelivered(ctx)
			if err != nil {
				slog.LogAttrs(ctx, slog.LevelWarn, "telemetry_stats_wal_count_error", slog.String("error", err.Error()))
			}

			slog.LogAttrs(
				ctx, slog.LevelInfo,
				"telemetry_stats",
				slog.Float64("ingest_rate_per_s", ingestRate),
				slog.Float64("send_rate_per_s", sendRate),
				slog.Int64("undelivered_count", undelivered),
				slog.Uint64("ingest_total", ingestTotal),
				slog.Uint64("sent_total", sentTotal),
				slog.Float64("interval_s", elapsed),
			)
		case <-ctx.Done():
			return
		}
	}
}
