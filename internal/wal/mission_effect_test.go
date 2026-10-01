package wal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestLegacyMissionWithoutOwnershipRemainsUncertainAcrossUpgrade(t *testing.T) {
	for _, state := range []string{"effect_started", "outcome_unknown"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "legacy.db")
			w, err := New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = w.ReserveMissionDeployment(ctx, "old", "fingerprint", []byte("mission")); err != nil {
				t.Fatal(err)
			}
			if _, err = w.db.Exec(`UPDATE mission_deployments SET state=? WHERE command_id='old'`, state); err != nil {
				t.Fatal(err)
			}
			// Simulate the schema preceding ownership tracking, then run normal startup.
			if _, err = w.db.Exec(`DROP TABLE mission_effect_ownership`); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err = w.MarkMissionDeploymentEffectStarted(ctx, "old", "fingerprint"); !errors.Is(err, ErrMissionEffectOwnershipUnknown) {
				t.Fatalf("missing ownership = %v", err)
			}
			record, err := w.LoadMissionDeployment(ctx, "old")
			if err != nil || record.State != state {
				t.Fatalf("history changed: %+v, %v", record, err)
			}
			var count int
			if err = w.db.QueryRow(`SELECT count(*) FROM mission_effect_ownership`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("invented ownership: %d, %v", count, err)
			}
		})
	}
}

func TestMissionReplacementMustRetainLatestEffectAcrossRestart(t *testing.T) {
	for _, paired := range []bool{false, true} {
		for _, newer := range []string{"none", "legacy", "durable"} {
			name := newer
			if paired {
				name += "-paired"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "mission.db")
				w, err := New(ctx, path, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if w != nil {
						_ = w.Close()
					}
				}()
				if _, _, err = w.ReserveMissionDeployment(ctx, "old", "fingerprint", []byte("mission")); err != nil {
					t.Fatal(err)
				}
				if paired {
					// Install the paired journal row directly: this regression exercises
					// effect ownership rather than envelope admission/encoding.
					if _, err = w.db.Exec(`INSERT INTO c2_commands(command_id,digest,payload,evidence) VALUES('old','digest',X'',X'')`); err != nil {
						t.Fatal(err)
					}
				}
				if err = w.MarkMissionDeploymentEffectStarted(ctx, "old", "fingerprint"); err != nil {
					t.Fatal(err)
				}
				if err = w.StoreMissionDeploymentResult(ctx, "old", "fingerprint", []byte("unknown"), true); err != nil {
					t.Fatal(err)
				}
				switch newer {
				case "legacy":
					err = w.RecordLegacyAircraftEffect(ctx)
				case "durable":
					err = w.AdmitCommand(ctx, "new", CommandRecord{Digest: "new", Payload: []byte{}, Evidence: []byte{}})
					if err == nil {
						var owned bool
						owned, err = w.BeginCommandEffect(ctx, "new", "new")
						if !owned && err == nil {
							t.Fatal("new command did not own effect")
						}
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
				w, err = New(ctx, path, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				err = w.MarkMissionDeploymentEffectStarted(ctx, "old", "fingerprint")
				if newer == "none" {
					if err != nil {
						t.Fatalf("latest same-command retry failed: %v", err)
					}
				} else if !errors.Is(err, ErrCommandSuperseded) {
					t.Fatalf("superseded replacement allowed: %v", err)
				}
			})
		}
	}
}
