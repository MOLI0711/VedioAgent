package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"mirrorcore/internal/application"
)

// URL signing lasts 15 minutes; the remaining 15 minutes lets requests that
// started just before URL expiry settle before the backend aborts the upload.
const uploadTerminationGrace = 30 * time.Minute

// RequestCancel persists intent before any OSS call. A running multipart
// initiation gets to settle under its operation lease; new part URLs stop as
// soon as the session leaves UPLOADING.
func (r *UploadRepository) RequestCancel(ctx context.Context, ownerID, sessionID uuid.UUID) (application.UploadSession, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.UploadSession{}, fmt.Errorf("begin upload cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockUploadOwner(ctx, tx, ownerID, sessionID); err != nil {
		return application.UploadSession{}, err
	}
	session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2 FOR UPDATE OF s`, sessionID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return application.UploadSession{}, uploadNotFound()
	}
	if err != nil {
		return application.UploadSession{}, fmt.Errorf("lock upload session for cancellation: %w", err)
	}
	switch session.Status {
	case "COMPLETE":
		return application.UploadSession{}, &application.ServiceError{Status: 409, Code: "UPLOAD_ALREADY_COMPLETE", Message: "上传已经完成，请删除视频资源"}
	case "ABORTED", "EXPIRED", "ABORTING":
		// Repeated DELETE is safe and returns the durable current state.
	case "INITIATING":
		_, err = tx.Exec(ctx, `UPDATE upload_sessions SET termination_reason=COALESCE(termination_reason,'CANCEL'),
			termination_after=GREATEST(COALESCE(termination_after,'-infinity'::timestamptz),
				clock_timestamp()+($3 * interval '1 millisecond'))
			WHERE id=$1 AND owner_id=$2 AND status='INITIATING'`, sessionID, ownerID, uploadTerminationGrace.Milliseconds())
		if err != nil {
			return application.UploadSession{}, fmt.Errorf("record upload cancellation request: %w", err)
		}
		if session.TerminationReason == "" {
			session.TerminationReason = "CANCEL"
		}
		session.CancellationRequested = true
	case "UPLOADING":
		_, err = tx.Exec(ctx, `UPDATE upload_sessions SET status='ABORTING',termination_reason='CANCEL',
			termination_after=clock_timestamp()+($3 * interval '1 millisecond'),
			operation_epoch=operation_epoch+1,operation_lease_until=NULL
			WHERE id=$1 AND owner_id=$2 AND status='UPLOADING'`, sessionID, ownerID, uploadTerminationGrace.Milliseconds())
		if err != nil {
			return application.UploadSession{}, fmt.Errorf("start upload cancellation: %w", err)
		}
		session.Status = "ABORTING"
		session.TerminationReason = "CANCEL"
		session.CancellationRequested = true
	case "COMPLETING", "VERIFYING":
		return application.UploadSession{}, &application.ServiceError{Status: 409, Code: "UPLOAD_COMPLETION_IN_PROGRESS", Message: "上传结果正在核实，请稍后查询后再取消"}
	default:
		return application.UploadSession{}, uploadStateConflict(session.Status)
	}
	if err := tx.Commit(ctx); err != nil {
		return application.UploadSession{}, fmt.Errorf("commit upload cancellation: %w", err)
	}
	return session, nil
}

// ClaimNextTermination waits for presigned URLs to expire and fences previous
// request handlers before returning an OSS multipart upload to abort.
func (r *UploadRepository) ClaimNextTermination(ctx context.Context) (*application.UploadSession, error) {
	var ownerID, sessionID uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT owner_id,id FROM upload_sessions
		WHERE status IN ('INITIATING','UPLOADING','ABORTING')
		  AND (termination_reason IS NOT NULL OR expires_at<=clock_timestamp())
		  AND COALESCE(termination_after,expires_at+interval '30 minutes')<=clock_timestamp()
		  AND (operation_lease_until IS NULL OR operation_lease_until<=clock_timestamp())
		ORDER BY COALESCE(termination_after,expires_at+interval '30 minutes'),id LIMIT 1`).
		Scan(&ownerID, &sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find upload session for termination: %w", err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin upload termination claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockUploadOwner(ctx, tx, ownerID, sessionID); err != nil {
		if serviceErr, ok := err.(*application.ServiceError); ok && serviceErr.Status == 404 {
			return nil, nil
		}
		return nil, err
	}
	session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2 FOR UPDATE OF s`, sessionID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock upload session for termination: %w", err)
	}
	var expired, terminationNotDue, leaseActive bool
	if err := tx.QueryRow(ctx, `SELECT expires_at<=clock_timestamp(),
		COALESCE(termination_after,expires_at+interval '30 minutes')>clock_timestamp(),
		operation_lease_until>clock_timestamp()
		FROM upload_sessions WHERE id=$1`, sessionID).
		Scan(&expired, &terminationNotDue, &leaseActive); err != nil {
		return nil, fmt.Errorf("check upload termination timing: %w", err)
	}
	if session.TerminationReason == "" {
		if !expired {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("finish upload termination recheck: %w", err)
			}
			return nil, nil
		}
		session.TerminationReason = "EXPIRED"
		err = tx.QueryRow(ctx, `UPDATE upload_sessions SET termination_reason='EXPIRED',
			termination_after=expires_at+interval '30 minutes'
			WHERE id=$1 AND owner_id=$2 RETURNING termination_after`, sessionID, ownerID).
			Scan(&session.TerminationAfter)
		if err != nil {
			return nil, fmt.Errorf("record expired upload cleanup: %w", err)
		}
	}
	if terminationNotDue {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("finish upload grace-period recheck: %w", err)
		}
		return nil, nil
	}
	if leaseActive {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("finish upload operation-lease recheck: %w", err)
		}
		return nil, nil
	}

	terminal, fileState := terminationResult(session.TerminationReason)
	if session.UploadID == "" {
		if _, err := tx.Exec(ctx, `UPDATE upload_sessions SET status=$3,parse_slot_reserved=false,
			operation_epoch=operation_epoch+1,operation_lease_until=NULL
			WHERE id=$1 AND owner_id=$2`, sessionID, ownerID, terminal); err != nil {
			return nil, fmt.Errorf("finish upload with no known OSS upload ID: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE videos SET file_state=$3 WHERE id=$1 AND owner_id=$2 AND file_state='UPLOADING'`,
			session.VideoID, ownerID, fileState); err != nil {
			return nil, fmt.Errorf("finish upload video without OSS upload ID: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit upload without OSS upload ID: %w", err)
		}
		return nil, nil
	}
	if session.Status == "UPLOADING" {
		session.OperationEpoch++
		var leaseUntil time.Time
		err := tx.QueryRow(ctx, `UPDATE upload_sessions SET status='ABORTING',operation_epoch=operation_epoch+1,
			operation_lease_until=clock_timestamp()+interval '2 minutes'
			WHERE id=$1 AND owner_id=$2 AND status='UPLOADING'
			RETURNING operation_lease_until`, sessionID, ownerID).Scan(&leaseUntil)
		if err != nil {
			return nil, fmt.Errorf("claim expired multipart upload: %w", err)
		}
		session.Status, session.OperationLeaseUntil = "ABORTING", &leaseUntil
	} else if session.Status == "ABORTING" {
		session.OperationEpoch++
		var leaseUntil time.Time
		err := tx.QueryRow(ctx, `UPDATE upload_sessions SET operation_epoch=operation_epoch+1,
			operation_lease_until=clock_timestamp()+interval '2 minutes'
			WHERE id=$1 AND owner_id=$2 AND status='ABORTING'
			RETURNING operation_lease_until`, sessionID, ownerID).Scan(&leaseUntil)
		if err != nil {
			return nil, fmt.Errorf("reclaim multipart abort: %w", err)
		}
		session.OperationLeaseUntil = &leaseUntil
	} else {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("finish stale upload termination candidate: %w", err)
		}
		return nil, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit multipart abort claim: %w", err)
	}
	return &session, nil
}

func (r *UploadRepository) FinishTermination(ctx context.Context, session application.UploadSession) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin upload termination: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockUploadOwner(ctx, tx, session.OwnerID, session.ID); err != nil {
		return err
	}
	terminal, fileState := terminationResult(session.TerminationReason)
	result, err := tx.Exec(ctx, `UPDATE upload_sessions SET status=$4,parse_slot_reserved=false,
		operation_lease_until=NULL WHERE id=$1 AND owner_id=$2 AND status='ABORTING'
		AND operation_epoch=$3 AND operation_lease_until>clock_timestamp()`,
		session.ID, session.OwnerID, session.OperationEpoch, terminal)
	if err != nil {
		return fmt.Errorf("finish OSS multipart abort: %w", err)
	}
	if result.RowsAffected() != 1 {
		return &application.ServiceError{Status: 409, Code: "UPLOAD_OPERATION_LOST", Message: "上传回收执行权已失效"}
	}
	if _, err := tx.Exec(ctx, `UPDATE videos SET file_state=$3 WHERE id=$1 AND owner_id=$2 AND file_state='UPLOADING'`,
		session.VideoID, session.OwnerID, fileState); err != nil {
		return fmt.Errorf("finish cancelled upload video state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit upload termination: %w", err)
	}
	return nil
}

func (r *UploadRepository) ReleaseTermination(ctx context.Context, session application.UploadSession) error {
	_, err := r.pool.Exec(ctx, `UPDATE upload_sessions SET operation_lease_until=NULL
		WHERE id=$1 AND status='ABORTING' AND operation_epoch=$2`, session.ID, session.OperationEpoch)
	if err != nil {
		return fmt.Errorf("release failed upload abort attempt: %w", err)
	}
	return nil
}

func terminationResult(reason string) (status, fileState string) {
	if reason == "EXPIRED" {
		return "EXPIRED", "EXPIRED"
	}
	return "ABORTED", "CANCELLED"
}
