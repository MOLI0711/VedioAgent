package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"mirrorcore/internal/application"
	"mirrorcore/internal/qa"
	"mirrorcore/internal/repository/postgres"
)

func (s *Server) createConversation(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "对话服务暂不可用", true, "", 1000)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	var request struct {
		Title *string `json:"title"`
	}
	if c.Request.ContentLength != 0 {
		if err := decodeJSON(c, &request); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请求格式不正确", false, "", 0)
			return
		}
	}
	conversation, created, err := s.questions.CreateConversation(c.Request.Context(), user.ID, videoID,
		c.GetHeader("Idempotency-Key"), request.Title)
	if err != nil {
		s.handleConversationError(c, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
		c.Header("Idempotency-Replayed", "true")
	}
	c.JSON(status, envelope{Data: conversation, RequestID: c.GetString("request_id")})
}

func (s *Server) lookupConversation(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "对话服务暂不可用", true, "", 1000)
		return
	}
	key := c.Query("client_request_key")
	if key == "" || len(key) > 128 {
		writeError(c, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "请求键无效", false, "", 0)
		return
	}
	conversation, err := s.questions.LookupConversation(c.Request.Context(), user.ID, key)
	if err != nil {
		s.handleConversationError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: conversation, RequestID: c.GetString("request_id")})
}

func (s *Server) listConversations(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "对话服务暂不可用", true, "", 1000)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	limit := 20
	if raw := c.Query("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeError(c, http.StatusBadRequest, "INVALID_LIMIT", "limit 必须在1到100之间", false, "", 0)
			return
		}
	}
	var cursorAt *time.Time
	var cursorID *uuid.UUID
	if raw := c.Query("cursor"); raw != "" {
		at, id, parseErr := application.ParseCursor(raw)
		if parseErr != nil {
			writeError(c, http.StatusBadRequest, "INVALID_CURSOR", "分页游标无效", false, "", 0)
			return
		}
		cursorAt, cursorID = &at, &id
	}
	page, err := s.questions.ListConversations(c.Request.Context(), user.ID, videoID, limit, cursorAt, cursorID)
	if err != nil {
		s.handleConversationError(c, err)
		return
	}
	response := gin.H{"conversations": page.Items}
	if page.NextUpdatedAt != nil && page.NextID != nil {
		response["next_cursor"] = application.EncodeCursor(*page.NextUpdatedAt, *page.NextID)
	}
	c.JSON(http.StatusOK, envelope{Data: response, RequestID: c.GetString("request_id")})
}

func (s *Server) listConversationQuestions(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "对话服务暂不可用", true, "", 1000)
		return
	}
	conversationID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "CONVERSATION_NOT_FOUND", "对话不存在", false, "", 0)
		return
	}
	items, err := s.questions.ListConversationQuestions(c.Request.Context(), user.ID, conversationID)
	if err != nil {
		s.handleConversationError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: gin.H{"questions": items}, RequestID: c.GetString("request_id")})
}

func (s *Server) archiveConversation(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "对话服务暂不可用", true, "", 1000)
		return
	}
	conversationID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "CONVERSATION_NOT_FOUND", "对话不存在", false, "", 0)
		return
	}
	interrupted, err := s.questions.ArchiveConversation(c.Request.Context(), user.ID, conversationID)
	if err != nil {
		s.handleConversationError(c, err)
		return
	}
	for _, questionID := range interrupted {
		s.questionCancelMu.Lock()
		cancel := s.questionCancels[questionID]
		s.questionCancelMu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	c.JSON(http.StatusOK, envelope{Data: gin.H{"conversation_id": conversationID, "archived": true,
		"interrupted_question_count": len(interrupted)}, RequestID: c.GetString("request_id")})
}

func (s *Server) handleConversationError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, postgres.ErrConversationNotFound), errors.Is(err, postgres.ErrQuestionNotFound):
		writeError(c, http.StatusNotFound, "NOT_FOUND", "对话或视频不存在", false, "", 0)
	case errors.Is(err, postgres.ErrConversationIdempotency):
		writeError(c, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "该请求键已用于不同的对话", false, "", 0)
	default:
		var runErr *qa.RunError
		if errors.As(err, &runErr) {
			writeError(c, http.StatusBadRequest, runErr.Code, runErr.Message, false, "", 0)
			return
		}
		s.logger.Error("conversation request failed", "request_id", c.GetString("request_id"), "error", err)
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法处理对话请求", true, "", 1000)
	}
}
