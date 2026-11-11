package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrConversationNotFound    = errors.New("conversation not found")
	ErrConversationIdempotency = errors.New("conversation key was reused with different content")
)

type Conversation struct {
	ID        uuid.UUID `json:"conversation_id"`
	OwnerID   uuid.UUID `json:"-"`
	VideoID   uuid.UUID `json:"video_id"`
	Title     *string   `json:"title,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ConversationInput struct {
	ID          uuid.UUID
	OwnerID     uuid.UUID
	VideoID     uuid.UUID
	RequestKey  string
	RequestHash []byte
	Title       *string
}

type ConversationPage struct {
	Items         []Conversation
	NextUpdatedAt *time.Time
	NextID        *uuid.UUID
}

type QuestionHistoryItem struct {
	ID          uuid.UUID       `json:"question_id"`
	TurnNo      *int64          `json:"turn_no,omitempty"`
	Question    string          `json:"question_text"`
	ResultID    *uuid.UUID      `json:"result_id,omitempty"`
	TaskVersion *int32          `json:"task_version,omitempty"`
	Status      string          `json:"execution_status"`
	Answer      json.RawMessage `json:"answer,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

func (r *QuestionRepository) CreateConversation(ctx context.Context, input ConversationInput) (Conversation, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Conversation{}, false, fmt.Errorf("begin conversation creation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, input.OwnerID).Scan(&locked); err != nil {
		return Conversation{}, false, fmt.Errorf("lock conversation owner: %w", err)
	}
	existing, existingHash, err := scanConversation(tx.QueryRow(ctx, `SELECT c.id,c.owner_id,c.video_id,c.title,c.created_at,c.updated_at,c.request_hash
		FROM conversations c JOIN videos v ON v.id=c.video_id AND v.owner_id=c.owner_id AND v.lifecycle='ACTIVE'
		WHERE c.owner_id=$1 AND c.create_request_key=$2 FOR UPDATE OF c`, input.OwnerID, input.RequestKey))
	if err == nil {
		if !bytes.Equal(existingHash, input.RequestHash) {
			return Conversation{}, false, ErrConversationIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return Conversation{}, false, fmt.Errorf("commit conversation replay lookup: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Conversation{}, false, fmt.Errorf("check conversation request key: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE' FOR UPDATE`,
		input.VideoID, input.OwnerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return Conversation{}, false, ErrQuestionNotFound
	} else if err != nil {
		return Conversation{}, false, fmt.Errorf("lock conversation video: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO conversations(id,owner_id,video_id,title,create_request_key,request_hash)
		VALUES($1,$2,$3,$4,$5,$6)`, input.ID, input.OwnerID, input.VideoID, input.Title, input.RequestKey, input.RequestHash)
	if err != nil {
		return Conversation{}, false, fmt.Errorf("insert conversation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Conversation{}, false, fmt.Errorf("commit conversation creation: %w", err)
	}
	created, err := r.GetConversation(ctx, input.OwnerID, input.ID)
	if err != nil {
		return Conversation{}, true, fmt.Errorf("conversation committed but reload failed: %w", err)
	}
	return created, true, nil
}

func scanConversation(row pgx.Row) (Conversation, []byte, error) {
	var item Conversation
	var requestHash []byte
	if err := row.Scan(&item.ID, &item.OwnerID, &item.VideoID, &item.Title, &item.CreatedAt, &item.UpdatedAt, &requestHash); err != nil {
		return Conversation{}, nil, err
	}
	return item, requestHash, nil
}

func (r *QuestionRepository) FindConversationByKey(ctx context.Context, ownerID uuid.UUID, key string) (Conversation, error) {
	var item Conversation
	err := r.pool.QueryRow(ctx, `SELECT c.id,c.owner_id,c.video_id,c.title,c.created_at,c.updated_at FROM conversations c
		JOIN videos v ON v.id=c.video_id AND v.owner_id=c.owner_id AND v.lifecycle='ACTIVE'
		WHERE c.owner_id=$1 AND c.create_request_key=$2`, ownerID, key).
		Scan(&item.ID, &item.OwnerID, &item.VideoID, &item.Title, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Conversation{}, ErrConversationNotFound
	}
	if err != nil {
		return Conversation{}, fmt.Errorf("find conversation by request key: %w", err)
	}
	return item, nil
}

func (r *QuestionRepository) GetConversation(ctx context.Context, ownerID, conversationID uuid.UUID) (Conversation, error) {
	var item Conversation
	err := r.pool.QueryRow(ctx, `SELECT c.id,c.owner_id,c.video_id,c.title,c.created_at,c.updated_at FROM conversations c
		JOIN videos v ON v.id=c.video_id AND v.owner_id=c.owner_id AND v.lifecycle='ACTIVE'
		WHERE c.owner_id=$1 AND c.id=$2`, ownerID, conversationID).
		Scan(&item.ID, &item.OwnerID, &item.VideoID, &item.Title, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Conversation{}, ErrConversationNotFound
	}
	if err != nil {
		return Conversation{}, fmt.Errorf("get conversation: %w", err)
	}
	return item, nil
}

func (r *QuestionRepository) ListConversations(ctx context.Context, ownerID, videoID uuid.UUID, limit int, cursorAt *time.Time, cursorID *uuid.UUID) (ConversationPage, error) {
	if limit < 1 || limit > 100 {
		return ConversationPage{}, errors.New("conversation limit must be between 1 and 100")
	}
	var exists bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM videos WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE')`,
		videoID, ownerID).Scan(&exists); err != nil {
		return ConversationPage{}, fmt.Errorf("check video before listing conversations: %w", err)
	} else if !exists {
		return ConversationPage{}, ErrQuestionNotFound
	}
	rows, err := r.pool.Query(ctx, `SELECT c.id,c.owner_id,c.video_id,c.title,c.created_at,c.updated_at FROM conversations c
		JOIN videos v ON v.id=c.video_id AND v.owner_id=c.owner_id AND v.lifecycle='ACTIVE'
		WHERE c.owner_id=$1 AND c.video_id=$2 AND c.archived_at IS NULL
		AND ($3::timestamptz IS NULL OR (c.updated_at,c.id)<($3,$4))
		ORDER BY c.updated_at DESC,c.id DESC LIMIT $5`, ownerID, videoID, cursorAt, cursorID, limit+1)
	if err != nil {
		return ConversationPage{}, fmt.Errorf("list video conversations: %w", err)
	}
	defer rows.Close()
	items := make([]Conversation, 0, limit+1)
	for rows.Next() {
		var item Conversation
		if err := rows.Scan(&item.ID, &item.OwnerID, &item.VideoID, &item.Title, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return ConversationPage{}, fmt.Errorf("scan conversation: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return ConversationPage{}, fmt.Errorf("iterate conversations: %w", err)
	}
	page := ConversationPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[len(page.Items)-1]
		page.NextUpdatedAt = &last.UpdatedAt
		page.NextID = &last.ID
	}
	return page, nil
}

func (r *QuestionRepository) ListConversationQuestions(ctx context.Context, ownerID, conversationID uuid.UUID) ([]QuestionHistoryItem, error) {
	if _, err := r.GetConversation(ctx, ownerID, conversationID); err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `SELECT q.id,q.turn_no,q.question_text,q.result_id,pr.task_version,q.status,
		q.answer_json,q.error,q.created_at,q.completed_at FROM questions q
		JOIN videos v ON v.id=q.video_id AND v.owner_id=q.owner_id AND v.lifecycle='ACTIVE'
		LEFT JOIN parse_results pr ON pr.id=q.result_id AND pr.video_id=q.video_id
		WHERE q.owner_id=$1 AND q.conversation_id=$2 ORDER BY q.turn_no`, ownerID, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list conversation questions: %w", err)
	}
	defer rows.Close()
	items := make([]QuestionHistoryItem, 0)
	for rows.Next() {
		var item QuestionHistoryItem
		if err := rows.Scan(&item.ID, &item.TurnNo, &item.Question, &item.ResultID, &item.TaskVersion,
			&item.Status, &item.Answer, &item.Error, &item.CreatedAt, &item.CompletedAt); err != nil {
			return nil, fmt.Errorf("scan conversation question: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conversation questions: %w", err)
	}
	return items, nil
}

func (r *QuestionRepository) ArchiveConversation(ctx context.Context, ownerID, conversationID uuid.UUID) ([]uuid.UUID, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin conversation archive: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var videoID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT video_id FROM conversations WHERE id=$1 AND owner_id=$2`,
		conversationID, ownerID).Scan(&videoID); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConversationNotFound
	} else if err != nil {
		return nil, fmt.Errorf("find conversation for archive: %w", err)
	}
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, ownerID).Scan(&locked); err != nil {
		return nil, fmt.Errorf("lock conversation owner for archive: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, videoID, ownerID).Scan(&locked); err != nil {
		return nil, fmt.Errorf("lock conversation video for archive: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM conversations WHERE id=$1 AND owner_id=$2 FOR UPDATE`,
		conversationID, ownerID).Scan(&locked); err != nil {
		return nil, fmt.Errorf("lock conversation for archive: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE conversations SET archived_at=COALESCE(archived_at,clock_timestamp()),updated_at=clock_timestamp()
		WHERE id=$1 AND owner_id=$2`, conversationID, ownerID); err != nil {
		return nil, fmt.Errorf("archive conversation: %w", err)
	}
	rows, err := tx.Query(ctx, `UPDATE questions SET status='INTERRUPTED',
		error='{"code":"CONVERSATION_ARCHIVED","message":"对话已归档"}'::jsonb,
		completed_at=clock_timestamp(),lease_until=NULL
		WHERE owner_id=$1 AND conversation_id=$2 AND status='RUNNING' RETURNING id`, ownerID, conversationID)
	if err != nil {
		return nil, fmt.Errorf("interrupt archived conversation questions: %w", err)
	}
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan interrupted question: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate interrupted questions: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit conversation archive: %w", err)
	}
	return ids, nil
}
