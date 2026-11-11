package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ASRManualResolution struct {
	SubmissionID   uuid.UUID
	OperatorID     string
	TerminalStatus string
	EvidenceRef    string
	Reason         string
}

func (r *ASRReconciliationRepository) ResolveUnknownWithoutTaskID(ctx context.Context,
	resolution ASRManualResolution) error {
	resolution.OperatorID = strings.TrimSpace(resolution.OperatorID)
	resolution.TerminalStatus = strings.ToUpper(strings.TrimSpace(resolution.TerminalStatus))
	resolution.EvidenceRef = strings.TrimSpace(resolution.EvidenceRef)
	resolution.Reason = strings.TrimSpace(resolution.Reason)
	if resolution.SubmissionID == uuid.Nil || len(resolution.OperatorID) == 0 || len(resolution.OperatorID) > 128 ||
		len(resolution.EvidenceRef) == 0 || len(resolution.EvidenceRef) > 512 ||
		len(resolution.Reason) == 0 || len(resolution.Reason) > 1000 || !manualTerminalStatus(resolution.TerminalStatus) {
		return errors.New("manual ASR resolution fields are invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin manual ASR resolution: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var ownerID, videoID, taskID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT v.owner_id,s.video_id,s.task_id FROM asr_submissions s
		JOIN videos v ON v.id=s.video_id WHERE s.id=$1`, resolution.SubmissionID).
		Scan(&ownerID, &videoID, &taskID); errors.Is(err, pgx.ErrNoRows) {
		return errors.New("ASR submission was not found")
	} else if err != nil {
		return fmt.Errorf("read ASR resolution ownership: %w", err)
	}

	var lockedID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, ownerID).Scan(&lockedID); err != nil {
		return fmt.Errorf("lock ASR resolution owner: %w", err)
	}
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT lifecycle FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, videoID, ownerID).
		Scan(&lifecycle); err != nil {
		return fmt.Errorf("lock ASR resolution video: %w", err)
	}
	var taskStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM parse_tasks WHERE id=$1 AND video_id=$2 FOR UPDATE`, taskID, videoID).
		Scan(&taskStatus); err != nil {
		return fmt.Errorf("lock ASR resolution parse task: %w", err)
	}
	if taskStatus == "FAILED" {
		var retryable bool
		if err := tx.QueryRow(ctx, `SELECT manual_retry_count < 1 AND (
			COALESCE(last_error->>'code'='TASK_DEADLINE_EXCEEDED',false) OR EXISTS(
				SELECT 1 FROM task_steps WHERE task_id=$1 AND state='FAILED' AND last_error->>'retryable'='true'
			)) FROM parse_tasks WHERE id=$1`, taskID).Scan(&retryable); err != nil {
			return fmt.Errorf("check ASR resolution task retryability: %w", err)
		}
		if retryable {
			return errors.New("manual ASR resolution requires the failed parse task to be no longer retryable")
		}
	}
	if taskStatus != "SUCCEEDED" && taskStatus != "FAILED" && taskStatus != "CANCELLED" &&
		lifecycle != "DELETING" && lifecycle != "DELETED" {
		return errors.New("manual ASR resolution requires a terminal parse task or a deleting video")
	}

	var state, providerTaskID string
	if err := tx.QueryRow(ctx, `SELECT state,COALESCE(provider_task_id,'') FROM asr_submissions WHERE id=$1 FOR UPDATE`,
		resolution.SubmissionID).Scan(&state, &providerTaskID); err != nil {
		return fmt.Errorf("lock ASR submission for manual resolution: %w", err)
	}
	if state == "REMOTE_TERMINAL" && providerTaskID == "" {
		var operatorID, terminalStatus, evidenceRef, reason string
		var superseded bool
		err := tx.QueryRow(ctx, `SELECT operator_id,terminal_status,evidence_ref,reason,superseded_at IS NOT NULL
			FROM asr_manual_resolutions WHERE submission_id=$1`, resolution.SubmissionID).
			Scan(&operatorID, &terminalStatus, &evidenceRef, &reason, &superseded)
		if err == nil && !superseded && operatorID == resolution.OperatorID &&
			terminalStatus == resolution.TerminalStatus && evidenceRef == resolution.EvidenceRef && reason == resolution.Reason {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit manual ASR resolution replay: %w", err)
			}
			return nil
		}
		return errors.New("ASR submission already has a different or superseded manual resolution")
	}
	if state != "UNKNOWN" || providerTaskID != "" {
		return errors.New("manual resolution is only allowed for UNKNOWN submissions without provider task ID")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO asr_manual_resolutions(submission_id,operator_id,terminal_status,evidence_ref,reason)
		VALUES($1,$2,$3,$4,$5)`, resolution.SubmissionID, resolution.OperatorID, resolution.TerminalStatus,
		resolution.EvidenceRef, resolution.Reason); err != nil {
		return fmt.Errorf("write manual ASR resolution audit: %w", err)
	}
	updated, err := tx.Exec(ctx, `UPDATE asr_submissions SET state='REMOTE_TERMINAL',
		provider_status='OPERATOR_CONFIRMED_'||$2,remote_terminal_at=clock_timestamp(),last_error=NULL,
		reconcile_owner=NULL,reconcile_lease_until=NULL,updated_at=clock_timestamp()
		WHERE id=$1 AND state='UNKNOWN' AND provider_task_id IS NULL`, resolution.SubmissionID, resolution.TerminalStatus)
	if err != nil {
		return fmt.Errorf("mark manually verified ASR terminal: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return errors.New("ASR submission changed before manual resolution")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit manual ASR resolution: %w", err)
	}
	return nil
}

func manualTerminalStatus(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED", "NOT_ACCEPTED":
		return true
	default:
		return false
	}
}
