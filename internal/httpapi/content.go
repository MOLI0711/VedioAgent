package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"mirrorcore/internal/content"
	"mirrorcore/internal/repository/postgres"
)

type contentJobResponse struct {
	ID             uuid.UUID  `json:"id"`
	VideoID        uuid.UUID  `json:"video_id"`
	ResultID       uuid.UUID  `json:"result_id"`
	Revision       int        `json:"revision"`
	State          string     `json:"state"`
	Stage          string     `json:"stage"`
	NextRunAt      string     `json:"next_run_at"`
	FirstStartedAt *string    `json:"first_started_at,omitempty"`
	DeadlineAt     *string    `json:"deadline_at,omitempty"`
	Error          *jsonError `json:"error,omitempty"`
	QueryURL       string     `json:"query_url"`
	CreatedAt      string     `json:"created_at"`
	CompletedAt    *string    `json:"completed_at,omitempty"`
}

type jsonError struct {
	Code      string `json:"code,omitempty"`
	Message   string `json:"message,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (s *Server) createContentJob(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.contents == nil {
		writeError(c, http.StatusServiceUnavailable, "CONTENT_SERVICE_DISABLED", "内容服务暂不可用", true, "", 1000)
		return
	}
	resultID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "解析结果不存在", false, "", 0)
		return
	}
	key := c.GetHeader("Idempotency-Key")
	job, created, err := s.contents.CreateRevision(c.Request.Context(), user.ID, resultID, key)
	if err != nil {
		s.handleContentError(c, err)
		return
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
		c.Header("Idempotency-Replayed", "true")
	}
	c.JSON(status, envelope{Data: toContentJobResponse(job), RequestID: c.GetString("request_id")})
}

func (s *Server) lookupContentJob(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.contents == nil {
		writeError(c, http.StatusServiceUnavailable, "CONTENT_SERVICE_DISABLED", "内容服务暂不可用", true, "", 1000)
		return
	}
	key := c.Query("client_request_key")
	if key == "" || len(key) > 128 {
		writeError(c, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "请求键无效", false, "", 0)
		return
	}
	job, err := s.contents.Lookup(c.Request.Context(), user.ID, key)
	if err != nil {
		s.handleContentError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: toContentJobResponse(job), RequestID: c.GetString("request_id")})
}

func (s *Server) getContentJob(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.contents == nil {
		writeError(c, http.StatusServiceUnavailable, "CONTENT_SERVICE_DISABLED", "内容服务暂不可用", true, "", 1000)
		return
	}
	jobID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "内容任务不存在", false, "", 0)
		return
	}
	job, err := s.contents.GetJob(c.Request.Context(), user.ID, jobID)
	if err != nil {
		s.handleContentError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: toContentJobResponse(job), RequestID: c.GetString("request_id")})
}

func (s *Server) getResultContent(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.contents == nil {
		writeError(c, http.StatusServiceUnavailable, "CONTENT_SERVICE_DISABLED", "内容服务暂不可用", true, "", 1000)
		return
	}
	resultID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "解析结果不存在", false, "", 0)
		return
	}
	view, err := s.contents.GetResult(c.Request.Context(), user.ID, resultID)
	if err != nil {
		s.handleContentError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: view, RequestID: c.GetString("request_id")})
}

func (s *Server) getContentArtifact(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.contents == nil {
		writeError(c, http.StatusServiceUnavailable, "CONTENT_SERVICE_DISABLED", "内容服务暂不可用", true, "", 1000)
		return
	}
	artifactID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "内容产物不存在", false, "", 0)
		return
	}
	payload, err := s.contents.GetArtifact(c.Request.Context(), user.ID, artifactID)
	if err != nil {
		s.handleContentError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: json.RawMessage(payload), RequestID: c.GetString("request_id")})
}

func (s *Server) handleContentError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, postgres.ErrContentJobNotFound), errors.Is(err, postgres.ErrContentNotFound):
		writeError(c, http.StatusNotFound, "NOT_FOUND", "内容任务、产物或解析结果不存在", false, "", 0)
	case errors.Is(err, postgres.ErrContentNoSpeech):
		writeError(c, http.StatusConflict, "NO_CONTENT", "该结果没有可供概览的语音内容", false, "", 0)
	case errors.Is(err, postgres.ErrContentInProgress):
		writeError(c, http.StatusConflict, "CONTENT_JOB_IN_PROGRESS", "当前结果已有内容任务正在处理", true, "", 1000)
	case errors.Is(err, postgres.ErrContentIdempotency):
		writeError(c, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "该请求键已用于不同的内容任务", false, "", 0)
	default:
		var runErr *content.RunError
		if errors.As(err, &runErr) {
			status := http.StatusBadRequest
			if runErr.Code == "CONTENT_JOB_IN_PROGRESS" || runErr.Code == "IDEMPOTENCY_KEY_REUSED" {
				status = http.StatusConflict
			} else if !runErr.Permanent {
				status = http.StatusServiceUnavailable
			}
			writeError(c, status, runErr.Code, runErr.Message, !runErr.Permanent, runErr.ResourceID, 1000)
			return
		}
		s.logger.Error("content request failed", "request_id", c.GetString("request_id"), "error", err)
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法处理内容请求", true, "", 1000)
	}
}

func toContentJobResponse(job postgres.ContentJob) contentJobResponse {
	response := contentJobResponse{ID: job.ID, VideoID: job.VideoID, ResultID: job.ResultID,
		Revision: job.Revision, State: job.State, Stage: job.Stage, NextRunAt: job.NextRunAt.UTC().Format(time.RFC3339Nano),
		QueryURL: "/v1/content-jobs/" + job.ID.String(), CreatedAt: job.CreatedAt.UTC().Format(time.RFC3339Nano)}
	if job.FirstStartedAt != nil {
		value := job.FirstStartedAt.UTC().Format(time.RFC3339Nano)
		response.FirstStartedAt = &value
	}
	if job.DeadlineAt != nil {
		value := job.DeadlineAt.UTC().Format(time.RFC3339Nano)
		response.DeadlineAt = &value
	}
	if job.CompletedAt != nil {
		value := job.CompletedAt.UTC().Format(time.RFC3339Nano)
		response.CompletedAt = &value
	}
	if len(job.Error) > 0 {
		var saved jsonError
		if json.Unmarshal(job.Error, &saved) == nil {
			response.Error = &saved
		}
	}
	return response
}
