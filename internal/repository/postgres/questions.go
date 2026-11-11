package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrQuestionNotFound        = errors.New("question not found")
	ErrQuestionContentNotReady = errors.New("video has no active parse result")
	ErrQuestionResultChanged   = errors.New("active parse result changed before question admission")
	ErrQuestionInProgress      = errors.New("user already has a running question")
	ErrQuestionConversation    = errors.New("conversation is missing, archived, or belongs to another video")
	ErrQuestionExecutionLost   = errors.New("question execution lease was lost")
	ErrQuestionIdempotency     = errors.New("idempotency key was reused with different content")
)

type Question struct {
	ID                uuid.UUID       `json:"question_id"`
	OwnerID           uuid.UUID       `json:"-"`
	VideoID           uuid.UUID       `json:"video_id"`
	ConversationID    *uuid.UUID      `json:"conversation_id,omitempty"`
	TurnNo            *int64          `json:"turn_no,omitempty"`
	QuestionText      string          `json:"question_text"`
	RequestKey        string          `json:"client_request_key"`
	RequestHash       []byte          `json:"-"`
	ResultID          *uuid.UUID      `json:"result_id,omitempty"`
	TaskVersion       *int32          `json:"task_version,omitempty"`
	ResultType        string          `json:"-"`
	TranscriptID      *uuid.UUID      `json:"-"`
	EmbeddingSetID    *uuid.UUID      `json:"-"`
	ContextIDs        []uuid.UUID     `json:"-"`
	ContextSnapshot   json.RawMessage `json:"-"`
	RetrievalQuery    string          `json:"-"`
	GenerationProfile json.RawMessage `json:"-"`
	RetrievalProfile  json.RawMessage `json:"-"`
	Status            string          `json:"execution_status"`
	ExecutionID       *uuid.UUID      `json:"-"`
	DeadlineAt        time.Time       `json:"-"`
	Answer            json.RawMessage `json:"answer,omitempty"`
	Error             json.RawMessage `json:"error,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
}

type AdmitQuestionInput struct {
	ID          uuid.UUID
	ExecutionID uuid.UUID
	OwnerID     uuid.UUID
	VideoID     uuid.UUID
	// RequiredResultID pins paired evaluation runs to the same active result.
	RequiredResultID  *uuid.UUID
	ConversationID    *uuid.UUID
	RequestKey        string
	QuestionText      string
	RequestHash       []byte
	OwnerInstance     string
	GenerationProfile []byte
	RetrievalProfile  []byte
}

type QuestionEvidence struct {
	ID           string
	TranscriptID uuid.UUID
	SentenceSeq  int
	Text         string
	StartMS      int64
	EndMS        int64
	ContentHash  []byte
}

type RetrievedSentence struct {
	TranscriptID uuid.UUID
	SentenceSeq  int
	Text         string
	StartMS      int64
	EndMS        int64
	Distance     float64
}

type QuestionRepository struct{ pool *pgxpool.Pool }

func NewQuestionRepository(pool *pgxpool.Pool) *QuestionRepository {
	return &QuestionRepository{pool: pool}
}

func (r *QuestionRepository) FindQuestionByKey(ctx context.Context, ownerID uuid.UUID, requestKey string) (Question, error) {
	item, err := scanQuestion(r.pool.QueryRow(ctx, questionSelect+` WHERE q.owner_id=$1 AND q.client_request_key=$2`, ownerID, requestKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return Question{}, ErrQuestionNotFound
	}
	return item, err
}

func (r *QuestionRepository) AdmitQuestion(ctx context.Context, input AdmitQuestionInput) (Question, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Question{}, false, fmt.Errorf("begin question admission: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, input.OwnerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return Question{}, false, ErrQuestionNotFound
	} else if err != nil {
		return Question{}, false, fmt.Errorf("lock question owner: %w", err)
	}
	existing, err := scanQuestion(tx.QueryRow(ctx, questionSelect+` WHERE q.owner_id=$1 AND q.client_request_key=$2 FOR UPDATE OF q`, input.OwnerID, input.RequestKey))
	if err == nil {
		if !bytes.Equal(existing.RequestHash, input.RequestHash) {
			return Question{}, false, ErrQuestionIdempotency
		}
		if input.RequiredResultID != nil && (existing.ResultID == nil || *existing.ResultID != *input.RequiredResultID) {
			return Question{}, false, ErrQuestionResultChanged
		}
		if err := tx.Commit(ctx); err != nil {
			return Question{}, false, fmt.Errorf("commit question replay lookup: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Question{}, false, fmt.Errorf("check question request key: %w", err)
	}
	var activeQuestions int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM questions WHERE owner_id=$1 AND status='RUNNING'
		AND lease_until>clock_timestamp()`, input.OwnerID).Scan(&activeQuestions); err != nil {
		return Question{}, false, fmt.Errorf("check user question concurrency: %w", err)
	}
	if activeQuestions >= 1 {
		return Question{}, false, ErrQuestionInProgress
	}
	var activeResultID *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT active_result_id FROM videos
		WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE' FOR UPDATE`, input.VideoID, input.OwnerID).Scan(&activeResultID); errors.Is(err, pgx.ErrNoRows) {
		return Question{}, false, ErrQuestionNotFound
	} else if err != nil {
		return Question{}, false, fmt.Errorf("lock question video: %w", err)
	}
	if activeResultID == nil {
		return Question{}, false, ErrQuestionContentNotReady
	}
	if input.RequiredResultID != nil && *activeResultID != *input.RequiredResultID {
		return Question{}, false, ErrQuestionResultChanged
	}
	var result Question
	if err := tx.QueryRow(ctx, `SELECT id,task_version,result_type,transcript_id,embedding_set_id
		FROM parse_results WHERE id=$1 AND video_id=$2`, *activeResultID, input.VideoID).
		Scan(&result.ResultID, &result.TaskVersion, &result.ResultType, &result.TranscriptID, &result.EmbeddingSetID); err != nil {
		return Question{}, false, fmt.Errorf("load active parse result: %w", err)
	}
	var turnNo *int64
	contextIDs := make([]uuid.UUID, 0)
	contextSnapshot := json.RawMessage(`[]`)
	if input.ConversationID != nil {
		if err := tx.QueryRow(ctx, `SELECT id FROM conversations WHERE id=$1 AND owner_id=$2 AND video_id=$3
			AND archived_at IS NULL FOR UPDATE`, *input.ConversationID, input.OwnerID, input.VideoID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
			return Question{}, false, ErrQuestionConversation
		} else if err != nil {
			return Question{}, false, fmt.Errorf("lock question conversation: %w", err)
		}
		turn := new(int64)
		if err := tx.QueryRow(ctx, `UPDATE conversations SET next_turn_no=next_turn_no+1,updated_at=clock_timestamp()
			WHERE id=$1 RETURNING next_turn_no-1`, *input.ConversationID).Scan(turn); err != nil {
			return Question{}, false, fmt.Errorf("allocate conversation turn: %w", err)
		}
		turnNo = turn
		contextIDs, contextSnapshot, err = loadQuestionContext(ctx, tx, *input.ConversationID)
		if err != nil {
			return Question{}, false, err
		}
	}
	retrievalQuery := buildRetrievalQuery(input.QuestionText, contextSnapshot)
	_, err = tx.Exec(ctx, `INSERT INTO questions(id,owner_id,video_id,conversation_id,turn_no,question_text,retrieval_query,
		context_question_ids,context_snapshot,client_request_key,request_hash,result_id,generation_profile,retrieval_profile,
		status,owner_instance,execution_id,lease_until,deadline_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13::jsonb,$14::jsonb,'RUNNING',$15,$16,
		clock_timestamp()+interval '45 seconds',clock_timestamp()+interval '180 seconds')`, input.ID, input.OwnerID,
		input.VideoID, input.ConversationID, turnNo, input.QuestionText, retrievalQuery, contextIDs, string(contextSnapshot),
		input.RequestKey, input.RequestHash, *activeResultID, string(input.GenerationProfile), string(input.RetrievalProfile),
		input.OwnerInstance, input.ExecutionID)
	if err != nil {
		return Question{}, false, fmt.Errorf("insert question: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		confirmCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		confirmed, confirmErr := r.FindQuestionByKey(confirmCtx, input.OwnerID, input.RequestKey)
		if confirmErr == nil && bytes.Equal(confirmed.RequestHash, input.RequestHash) {
			return confirmed, false, nil
		}
		return Question{}, false, fmt.Errorf("commit question admission outcome is unknown: %w", err)
	}
	question, err := r.FindQuestionByKey(ctx, input.OwnerID, input.RequestKey)
	if err != nil {
		// The commit has succeeded; callers must resolve this stable request key before retrying.
		return Question{}, true, fmt.Errorf("question committed but reload failed: %w", err)
	}
	return question, true, nil
}

const questionSelect = `SELECT q.id,q.owner_id,q.video_id,q.conversation_id,q.turn_no,q.question_text,q.client_request_key,
	q.request_hash,q.result_id,pr.task_version,pr.result_type,pr.transcript_id,pr.embedding_set_id,
	q.context_question_ids,q.context_snapshot,q.retrieval_query,q.generation_profile,q.retrieval_profile,q.status,
	q.execution_id,q.deadline_at,q.answer_json,q.error,q.created_at,q.completed_at
	FROM questions q JOIN videos v ON v.id=q.video_id AND v.owner_id=q.owner_id AND v.lifecycle='ACTIVE'
	LEFT JOIN parse_results pr ON pr.id=q.result_id AND pr.video_id=q.video_id`

func scanQuestion(row pgx.Row) (Question, error) {
	var item Question
	err := row.Scan(&item.ID, &item.OwnerID, &item.VideoID, &item.ConversationID, &item.TurnNo, &item.QuestionText,
		&item.RequestKey, &item.RequestHash, &item.ResultID, &item.TaskVersion, &item.ResultType, &item.TranscriptID,
		&item.EmbeddingSetID, &item.ContextIDs, &item.ContextSnapshot, &item.RetrievalQuery, &item.GenerationProfile,
		&item.RetrievalProfile, &item.Status, &item.ExecutionID, &item.DeadlineAt, &item.Answer, &item.Error,
		&item.CreatedAt, &item.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Question{}, pgx.ErrNoRows
	}
	if err != nil {
		return Question{}, err
	}
	return item, nil
}

func (r *QuestionRepository) GetQuestion(ctx context.Context, ownerID, questionID uuid.UUID) (Question, error) {
	item, err := scanQuestion(r.pool.QueryRow(ctx, questionSelect+` WHERE q.owner_id=$1 AND q.id=$2`, ownerID, questionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Question{}, ErrQuestionNotFound
	}
	return item, err
}

func (r *QuestionRepository) SearchEvidence(ctx context.Context, videoID, transcriptID, embeddingSetID uuid.UUID, vector []float32, topK int) ([]RetrievedSentence, error) {
	if len(vector) != 1024 || topK < 1 || topK > 8 {
		return nil, errors.New("invalid retrieval vector or top-k")
	}
	vectorText := formatQuestionVector(vector)
	rows, err := r.pool.Query(ctx, `WITH nearest AS (
		SELECT c.id,c.seq,c.start_ms,c.end_ms,ce.embedding <=> $4::vector(1024) AS distance
		FROM chunk_embeddings ce JOIN chunks c ON c.id=ce.chunk_id AND c.chunk_set_id=ce.chunk_set_id AND c.video_id=ce.video_id
		WHERE ce.embedding_set_id=$1 AND ce.video_id=$2 AND ce.chunk_set_id=(
			SELECT chunk_set_id FROM embedding_sets WHERE id=$1 AND video_id=$2
		) ORDER BY ce.embedding <=> $4::vector(1024),c.seq,c.id LIMIT $5
	)
	SELECT ts.transcript_id,ts.seq,ts.text,ts.start_ms,ts.end_ms,nearest.distance
	FROM nearest JOIN chunk_sources cs ON cs.chunk_id=nearest.id AND cs.video_id=$2 AND cs.transcript_id=$3
	JOIN transcript_segments ts ON ts.transcript_id=cs.transcript_id AND ts.video_id=cs.video_id
		AND ts.seq=cs.transcript_segment_seq
	ORDER BY nearest.distance,nearest.seq,ts.seq`, embeddingSetID, videoID, transcriptID, vectorText, topK)
	if err != nil {
		return nil, fmt.Errorf("search fixed result embeddings: %w", err)
	}
	defer rows.Close()
	items := make([]RetrievedSentence, 0, topK*2)
	for rows.Next() {
		var item RetrievedSentence
		if err := rows.Scan(&item.TranscriptID, &item.SentenceSeq, &item.Text, &item.StartMS, &item.EndMS, &item.Distance); err != nil {
			return nil, fmt.Errorf("scan retrieved source sentence: %w", err)
		}
		if math.IsNaN(item.Distance) || math.IsInf(item.Distance, 0) || !utf8.ValidString(item.Text) || item.Text == "" {
			return nil, errors.New("retrieved source sentence has invalid content")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retrieved source sentences: %w", err)
	}
	return items, nil
}

func (r *QuestionRepository) ExpandEvidence(ctx context.Context, transcriptID uuid.UUID, sentenceSeqs []int) ([]RetrievedSentence, error) {
	if len(sentenceSeqs) == 0 || len(sentenceSeqs) > 24 {
		return nil, errors.New("evidence expansion requires between 1 and 24 sentence IDs")
	}
	rows, err := r.pool.Query(ctx, `WITH requested AS (SELECT unnest($2::integer[]) AS seq),
	candidates AS (
		SELECT DISTINCT ON (ts.seq) ts.transcript_id,ts.seq,ts.text,ts.start_ms,ts.end_ms,
			abs(ts.seq-requested.seq) AS distance
		FROM requested JOIN transcript_segments ts ON ts.transcript_id=$1
			AND ts.seq BETWEEN requested.seq-1 AND requested.seq+1
		ORDER BY ts.seq,abs(ts.seq-requested.seq),requested.seq
	)
	SELECT transcript_id,seq,text,start_ms,end_ms,distance::double precision
	FROM candidates ORDER BY seq`, transcriptID, sentenceSeqs)
	if err != nil {
		return nil, fmt.Errorf("expand adjacent source sentences: %w", err)
	}
	defer rows.Close()
	items := make([]RetrievedSentence, 0, len(sentenceSeqs)*3)
	for rows.Next() {
		var item RetrievedSentence
		if err := rows.Scan(&item.TranscriptID, &item.SentenceSeq, &item.Text, &item.StartMS, &item.EndMS, &item.Distance); err != nil {
			return nil, fmt.Errorf("scan expanded source sentence: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expanded source sentences: %w", err)
	}
	return items, nil
}

func (r *QuestionRepository) ReadEvidenceContext(ctx context.Context, transcriptID uuid.UUID, sentenceSeq, before, after int) ([]RetrievedSentence, error) {
	if transcriptID == uuid.Nil || sentenceSeq <= 0 || before < 0 || before > 3 || after < 0 || after > 3 || before+after == 0 {
		return nil, errors.New("invalid evidence context range")
	}
	rows, err := r.pool.Query(ctx, `SELECT transcript_id,seq,text,start_ms,end_ms,0::double precision
		FROM transcript_segments WHERE transcript_id=$1 AND seq BETWEEN GREATEST(1,$2::integer-$3::integer) AND $2::integer+$4::integer
		ORDER BY seq LIMIT 7`, transcriptID, sentenceSeq, before, after)
	if err != nil {
		return nil, fmt.Errorf("read fixed transcript context: %w", err)
	}
	defer rows.Close()
	items := make([]RetrievedSentence, 0, before+after+1)
	for rows.Next() {
		var item RetrievedSentence
		if err := rows.Scan(&item.TranscriptID, &item.SentenceSeq, &item.Text, &item.StartMS, &item.EndMS, &item.Distance); err != nil {
			return nil, fmt.Errorf("scan fixed transcript context: %w", err)
		}
		if item.TranscriptID != transcriptID || item.SentenceSeq <= 0 || item.Text == "" ||
			!utf8.ValidString(item.Text) || item.StartMS < 0 || item.EndMS <= item.StartMS {
			return nil, errors.New("fixed transcript context contains invalid source data")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fixed transcript context: %w", err)
	}
	return items, nil
}

func (r *QuestionRepository) RegisterEvidence(ctx context.Context, item Question, evidence []QuestionEvidence) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin evidence registration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockQuestionExecution(ctx, tx, item); err != nil {
		return err
	}
	for _, source := range evidence {
		sum := sha256.Sum256([]byte(source.Text))
		if source.ID == "" || source.TranscriptID == uuid.Nil || source.SentenceSeq <= 0 || source.Text == "" ||
			!utf8.ValidString(source.Text) || source.StartMS < 0 || source.EndMS <= source.StartMS || len(source.ContentHash) != sha256.Size ||
			!bytes.Equal(sum[:], source.ContentHash) {
			return errors.New("evidence source identity is invalid")
		}
		_, err := tx.Exec(ctx, `INSERT INTO question_evidence(question_id,video_id,result_id,transcript_id,evidence_id,
			sentence_seq,start_byte,end_byte,original_text,start_ms,end_ms,content_hash)
			VALUES($1,$2,$3,$4,$5,$6,0,octet_length($7),$7,$8,$9,$10)
			ON CONFLICT(question_id,evidence_id) DO NOTHING`, item.ID, item.VideoID, *item.ResultID,
			source.TranscriptID, source.ID, source.SentenceSeq, source.Text, source.StartMS, source.EndMS, source.ContentHash)
		if err != nil {
			return fmt.Errorf("register question evidence: %w", err)
		}
		var transcriptID uuid.UUID
		var sentenceSeq int
		var text string
		var contentHash []byte
		if err := tx.QueryRow(ctx, `SELECT transcript_id,sentence_seq,original_text,content_hash FROM question_evidence
			WHERE question_id=$1 AND evidence_id=$2`, item.ID, source.ID).Scan(&transcriptID, &sentenceSeq, &text, &contentHash); err != nil {
			return fmt.Errorf("verify registered question evidence: %w", err)
		}
		if transcriptID != source.TranscriptID || sentenceSeq != source.SentenceSeq || text != source.Text || !bytes.Equal(contentHash, source.ContentHash) {
			return errors.New("question evidence ID already refers to different source text")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit question evidence: %w", err)
	}
	return nil
}

func (r *QuestionRepository) BeginQuestionStep(ctx context.Context, item Question, seq int, kind string, inputHash []byte) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin question step reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockQuestionExecution(ctx, tx, item); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO question_steps(question_id,seq,kind,input_hash)
		VALUES($1,$2,$3,$4) ON CONFLICT(question_id,seq) DO NOTHING`, item.ID, seq, kind, inputHash)
	if err != nil {
		return fmt.Errorf("reserve question step: %w", err)
	}
	var savedHash []byte
	if err := tx.QueryRow(ctx, `SELECT input_hash FROM question_steps WHERE question_id=$1 AND seq=$2`, item.ID, seq).Scan(&savedHash); err != nil {
		return fmt.Errorf("verify question step reservation: %w", err)
	}
	if !bytes.Equal(savedHash, inputHash) {
		return errors.New("question step sequence already belongs to different input")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit question step reservation: %w", err)
	}
	return nil
}

func (r *QuestionRepository) CompleteQuestionStep(ctx context.Context, item Question, seq int, inputHash []byte, providerRequestID string, promptTokens, completionTokens, usageTokens int64, outcomeCode string) error {
	return r.CompleteQuestionStepWithMetadata(ctx, item, seq, inputHash, providerRequestID,
		promptTokens, completionTokens, usageTokens, outcomeCode, nil)
}

func (r *QuestionRepository) CompleteQuestionStepWithMetadata(ctx context.Context, item Question, seq int, inputHash []byte,
	providerRequestID string, promptTokens, completionTokens, usageTokens int64, outcomeCode string, metadata []byte) error {
	if len(metadata) == 0 {
		metadata = []byte(`{}`)
	}
	if !json.Valid(metadata) {
		return errors.New("question step metadata must be valid JSON")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin question step: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockQuestionExecution(ctx, tx, item); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE question_steps SET provider_request_id=NULLIF($4,''),completed_at=clock_timestamp(),
		prompt_tokens=NULLIF($5,0),completion_tokens=NULLIF($6,0),usage_tokens=NULLIF($7,0),outcome_code=NULLIF($8,''),metadata=$9::jsonb
		WHERE question_id=$1 AND seq=$2 AND input_hash=$3 AND completed_at IS NULL`, item.ID, seq, inputHash,
		providerRequestID, promptTokens, completionTokens, usageTokens, outcomeCode, string(metadata))
	if err != nil {
		return fmt.Errorf("complete question step: %w", err)
	}
	if result.RowsAffected() == 0 {
		var savedHash []byte
		var completedAt *time.Time
		if err := tx.QueryRow(ctx, `SELECT input_hash,completed_at FROM question_steps WHERE question_id=$1 AND seq=$2`,
			item.ID, seq).Scan(&savedHash, &completedAt); err != nil || !bytes.Equal(savedHash, inputHash) || completedAt == nil {
			return errors.New("question step completion could not be confirmed")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit question step: %w", err)
	}
	return nil
}

func (r *QuestionRepository) RenewQuestionLease(ctx context.Context, item Question) (bool, error) {
	result, err := r.pool.Exec(ctx, `UPDATE questions SET lease_until=clock_timestamp()+interval '45 seconds'
		WHERE id=$1 AND owner_id=$2 AND status='RUNNING' AND execution_id=$3
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`, item.ID, item.OwnerID, item.ExecutionID)
	if err != nil {
		return false, fmt.Errorf("renew question execution lease: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func (r *QuestionRepository) CommitQuestionSuccess(ctx context.Context, item Question, answer []byte) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin question completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockQuestionExecution(ctx, tx, item); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE questions SET status='SUCCEEDED',answer_json=$4::jsonb,
		error=NULL,completed_at=clock_timestamp(),lease_until=NULL
		WHERE id=$1 AND owner_id=$2 AND execution_id=$3 AND status='RUNNING'
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`, item.ID, item.OwnerID, item.ExecutionID, string(answer))
	if err != nil {
		return fmt.Errorf("save question answer: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrQuestionExecutionLost
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit question answer: %w", err)
	}
	return nil
}

func (r *QuestionRepository) FinishQuestion(ctx context.Context, item Question, status, code, message string) error {
	if status != "FAILED" && status != "INTERRUPTED" {
		return errors.New("question finish state must be FAILED or INTERRUPTED")
	}
	errorJSON, _ := json.Marshal(map[string]string{"code": code, "message": message})
	_, err := r.pool.Exec(ctx, `UPDATE questions SET status=$4,error=$5::jsonb,completed_at=clock_timestamp(),lease_until=NULL
		WHERE id=$1 AND owner_id=$2 AND execution_id=$3 AND status='RUNNING'`, item.ID, item.OwnerID,
		item.ExecutionID, status, string(errorJSON))
	if err != nil {
		return fmt.Errorf("finish question execution: %w", err)
	}
	return nil
}

func (r *QuestionRepository) CancelQuestion(ctx context.Context, ownerID, questionID uuid.UUID) (string, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", fmt.Errorf("begin question cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var videoID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT video_id FROM questions WHERE id=$1 AND owner_id=$2`, questionID, ownerID).Scan(&videoID); errors.Is(err, pgx.ErrNoRows) {
		return "", ErrQuestionNotFound
	} else if err != nil {
		return "", fmt.Errorf("find question for cancellation: %w", err)
	}
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, ownerID).Scan(&locked); err != nil {
		return "", fmt.Errorf("lock question owner for cancellation: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, videoID, ownerID).Scan(&locked); err != nil {
		return "", fmt.Errorf("lock question video for cancellation: %w", err)
	}
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM questions WHERE id=$1 AND owner_id=$2 FOR UPDATE`, questionID, ownerID).Scan(&status); err != nil {
		return "", fmt.Errorf("lock question for cancellation: %w", err)
	}
	if status == "RUNNING" {
		_, err := tx.Exec(ctx, `UPDATE questions SET status='INTERRUPTED',error='{"code":"CANCELLED","message":"问答已取消"}'::jsonb,
			completed_at=clock_timestamp(),lease_until=NULL WHERE id=$1 AND owner_id=$2 AND status='RUNNING'`, questionID, ownerID)
		if err != nil {
			return "", fmt.Errorf("interrupt question: %w", err)
		}
		status = "INTERRUPTED"
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit question cancellation: %w", err)
	}
	return status, nil
}

func (r *QuestionRepository) ReapExpiredQuestions(ctx context.Context) (int64, error) {
	result, err := r.pool.Exec(ctx, `UPDATE questions SET
		status=CASE WHEN deadline_at<=statement_timestamp() THEN 'FAILED' ELSE 'INTERRUPTED' END,
		error=CASE WHEN deadline_at<=statement_timestamp()
			THEN '{"code":"QA_TIMEOUT","message":"问答超过总处理时限"}'::jsonb
			ELSE '{"code":"EXECUTION_LOST","message":"问答执行租约已过期"}'::jsonb END,
		completed_at=clock_timestamp(),lease_until=NULL
		WHERE status='RUNNING' AND (lease_until<=statement_timestamp() OR deadline_at<=statement_timestamp())`)
	if err != nil {
		return 0, fmt.Errorf("reap expired questions: %w", err)
	}
	return result.RowsAffected(), nil
}

func lockQuestionExecution(ctx context.Context, tx pgx.Tx, item Question) error {
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, item.OwnerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return ErrQuestionExecutionLost
	} else if err != nil {
		return fmt.Errorf("lock question owner: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE' FOR UPDATE`,
		item.VideoID, item.OwnerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return ErrQuestionExecutionLost
	} else if err != nil {
		return fmt.Errorf("lock question video: %w", err)
	}
	var status string
	var executionID *uuid.UUID
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT status,execution_id,
		lease_until>clock_timestamp() AND deadline_at>clock_timestamp()
		FROM questions WHERE id=$1 AND owner_id=$2 AND video_id=$3 FOR UPDATE`,
		item.ID, item.OwnerID, item.VideoID).Scan(&status, &executionID, &valid); errors.Is(err, pgx.ErrNoRows) {
		return ErrQuestionExecutionLost
	} else if err != nil {
		return fmt.Errorf("lock question execution: %w", err)
	}
	if status != "RUNNING" || executionID == nil || item.ExecutionID == nil || *executionID != *item.ExecutionID || !valid {
		return ErrQuestionExecutionLost
	}
	return nil
}

func loadQuestionContext(ctx context.Context, tx pgx.Tx, conversationID uuid.UUID) ([]uuid.UUID, json.RawMessage, error) {
	rows, err := tx.Query(ctx, `SELECT id,question_text,answer_json FROM questions
		WHERE conversation_id=$1 AND status='SUCCEEDED' AND answer_json IS NOT NULL AND purged_at IS NULL
		ORDER BY turn_no DESC LIMIT 2`, conversationID)
	if err != nil {
		return nil, nil, fmt.Errorf("load prior conversation turns: %w", err)
	}
	defer rows.Close()
	type turn struct {
		ID       uuid.UUID `json:"question_id"`
		Question string    `json:"question"`
		Answer   string    `json:"answer"`
	}
	turns := make([]turn, 0, 2)
	usedBytes := 0
	for rows.Next() {
		var id uuid.UUID
		var question string
		var rawAnswer []byte
		if err := rows.Scan(&id, &question, &rawAnswer); err != nil {
			return nil, nil, fmt.Errorf("scan prior conversation turn: %w", err)
		}
		answer := conciseAnswer(rawAnswer)
		turnBytes := len(question) + len(answer)
		if usedBytes+turnBytes > 8192 {
			continue
		}
		usedBytes += turnBytes
		turns = append(turns, turn{ID: id, Question: question, Answer: answer})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate prior conversation turns: %w", err)
	}
	for left, right := 0, len(turns)-1; left < right; left, right = left+1, right-1 {
		turns[left], turns[right] = turns[right], turns[left]
	}
	ids := make([]uuid.UUID, len(turns))
	for i := range turns {
		ids[i] = turns[i].ID
	}
	snapshot, err := json.Marshal(turns)
	if err != nil {
		return nil, nil, fmt.Errorf("encode question context snapshot: %w", err)
	}
	return ids, snapshot, nil
}

func conciseAnswer(raw []byte) string {
	var answer struct {
		Status       string `json:"status"`
		AnswerBlocks []struct {
			Text string `json:"text"`
		} `json:"answer_blocks"`
		UnansweredPoints []string `json:"unanswered_points"`
	}
	if json.Unmarshal(raw, &answer) != nil {
		return ""
	}
	parts := make([]string, 0, len(answer.AnswerBlocks)+len(answer.UnansweredPoints))
	for _, block := range answer.AnswerBlocks {
		if block.Text != "" {
			parts = append(parts, block.Text)
		}
	}
	parts = append(parts, answer.UnansweredPoints...)
	return strings.Join(parts, "\n")
}

func buildRetrievalQuery(question string, snapshot json.RawMessage) string {
	var turns []struct {
		Question string `json:"question"`
		Answer   string `json:"answer"`
	}
	_ = json.Unmarshal(snapshot, &turns)
	var b strings.Builder
	for _, turn := range turns {
		if turn.Question != "" {
			b.WriteString(turn.Question)
			b.WriteByte('\n')
		}
	}
	b.WriteString(question)
	value := b.String()
	if len(value) <= 4096 {
		return value
	}
	value = question
	if len(value) > 4096 {
		value = truncateUTF8(value, 4096)
	}
	return value
}

func truncateUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func formatQuestionVector(vector []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, value := range vector {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

func (r *QuestionRepository) EvidenceForAnswer(ctx context.Context, questionID uuid.UUID, evidenceIDs []string) ([]QuestionEvidence, error) {
	if len(evidenceIDs) == 0 {
		return nil, nil
	}
	rows, err := r.pool.Query(ctx, `SELECT evidence_id,transcript_id,sentence_seq,original_text,start_ms,end_ms,content_hash
		FROM question_evidence WHERE question_id=$1 AND evidence_id=ANY($2) ORDER BY evidence_id`, questionID, evidenceIDs)
	if err != nil {
		return nil, fmt.Errorf("load answer evidence: %w", err)
	}
	defer rows.Close()
	items := make([]QuestionEvidence, 0, len(evidenceIDs))
	for rows.Next() {
		var item QuestionEvidence
		if err := rows.Scan(&item.ID, &item.TranscriptID, &item.SentenceSeq, &item.Text,
			&item.StartMS, &item.EndMS, &item.ContentHash); err != nil {
			return nil, fmt.Errorf("scan answer evidence: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate answer evidence: %w", err)
	}
	return items, nil
}
