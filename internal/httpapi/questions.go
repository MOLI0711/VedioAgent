package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"mirrorcore/internal/qa"
	"mirrorcore/internal/repository/postgres"
)

type askQuestionRequest struct {
	ConversationID *uuid.UUID `json:"conversation_id"`
	Question       string     `json:"question"`
}

func (s *Server) askQuestion(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "问答服务暂不可用", true, "", 1000)
		return
	}
	if s.questionExecutionsStopping() {
		writeError(c, http.StatusServiceUnavailable, "API_SHUTTING_DOWN", "服务正在关闭，请稍后重试", true, "", 1000)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	var request askQuestionRequest
	if err := decodeJSON(c, &request); err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请求格式不正确", false, "", 0)
		return
	}
	requestKey := c.GetHeader("Idempotency-Key")
	input := qa.CreateInput{OwnerID: user.ID, VideoID: videoID, ConversationID: request.ConversationID,
		RequestKey: requestKey, Question: request.Question}
	_, lookupErr := s.questions.FindByKey(c.Request.Context(), user.ID, requestKey)
	reserved := false
	if errors.Is(lookupErr, postgres.ErrQuestionNotFound) {
		select {
		case s.questionSlots <- struct{}{}:
			reserved = true
		default:
			writeError(c, http.StatusTooManyRequests, "QUESTION_CAPACITY_FULL", "问答服务繁忙，请稍后重试", true, "", 1000)
			return
		}
	} else if lookupErr != nil {
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法查询问答请求", true, "", 1000)
		return
	}
	question, created, err := s.questions.Admit(c.Request.Context(), input)
	if err != nil {
		if reserved {
			<-s.questionSlots
		}
		s.handleQuestionAdmissionError(c, err)
		return
	}
	if !created && reserved {
		<-s.questionSlots
		reserved = false
	}
	if !created {
		s.replayOrRejectQuestion(c, question)
		return
	}
	if reserved {
		defer func() { <-s.questionSlots }()
	}
	if question.Status != "RUNNING" {
		s.replayOrRejectQuestion(c, question)
		return
	}
	runCtx, cancel := context.WithTimeout(c.Request.Context(), 180*time.Second)
	if !s.registerQuestionCancel(question.ID, cancel) {
		cancel()
		s.finishDisconnectedQuestion(question, "INTERRUPTED", "API_SHUTTING_DOWN", "服务关闭，问答已中断")
		writeError(c, http.StatusServiceUnavailable, "API_SHUTTING_DOWN", "服务正在关闭，请稍后重试", true,
			question.ID.String(), 1000)
		return
	}
	defer func() {
		s.unregisterQuestionCancel(question.ID)
		cancel()
	}()
	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache, no-transform")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	if err := writeQuestionEvent(c, "accepted", gin.H{
		"question_id": question.ID, "conversation_id": question.ConversationID,
		"client_request_key": question.RequestKey, "result_id": question.ResultID,
		"query_url": "/v1/questions/" + question.ID.String(), "request_id": c.GetString("request_id"),
	}); err != nil {
		s.finishDisconnectedQuestion(question, "INTERRUPTED", "CLIENT_DISCONNECTED", "客户端连接已断开")
		return
	}
	leaseDone := make(chan struct{})
	go s.renewQuestionLease(runCtx, cancel, question, leaseDone)
	type runOutcome struct {
		answer qa.Answer
		err    error
	}
	progressEvents := make(chan string)
	runDone := make(chan runOutcome, 1)
	go func() {
		answer, runErr := s.questions.Run(runCtx, question, func(stage string) error {
			select {
			case progressEvents <- stage:
				return nil
			case <-runCtx.Done():
				return runCtx.Err()
			}
		})
		runDone <- runOutcome{answer: answer, err: runErr}
	}()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	var progressSeq int
	clientGone := false
	ctxDone := runCtx.Done()
	var outcome runOutcome
	for {
		select {
		case stage := <-progressEvents:
			if clientGone {
				continue
			}
			progressSeq++
			if err := writeQuestionEvent(c, "progress", gin.H{"question_id": question.ID,
				"seq": progressSeq, "stage": stage}); err != nil {
				clientGone = true
				cancel()
			}
		case outcome = <-runDone:
			goto questionFinished
		case <-ctxDone:
			ctxDone = nil
		case <-heartbeat.C:
			if !clientGone && runCtx.Err() == nil {
				if err := writeQuestionHeartbeat(c); err != nil {
					clientGone = true
					cancel()
				}
			}
		}
	}
questionFinished:
	contextErr := runCtx.Err()
	cancel()
	<-leaseDone
	if outcome.err != nil {
		status, code, message := questionFailure(contextErr, outcome.err)
		if !errors.Is(outcome.err, postgres.ErrQuestionExecutionLost) {
			s.finishDisconnectedQuestion(question, status, code, message)
		}
		if !clientGone && c.Request.Context().Err() == nil {
			_ = writeQuestionEvent(c, "error", gin.H{"question_id": question.ID, "code": code,
				"query_url": "/v1/questions/" + question.ID.String()})
		}
		return
	}
	if clientGone {
		return
	}
	if err := writeQuestionEvent(c, "answer", gin.H{"question_id": question.ID, "answer": outcome.answer}); err != nil {
		return
	}
	_ = writeQuestionEvent(c, "done", gin.H{"question_id": question.ID, "status": "SUCCEEDED"})
}

func (s *Server) replayOrRejectQuestion(c *gin.Context, question postgres.Question) {
	switch question.Status {
	case "SUCCEEDED":
		if len(question.Answer) == 0 || !json.Valid(question.Answer) {
			writeError(c, http.StatusGone, "QUESTION_RESULT_UNAVAILABLE", "问答结果已不可用", false, question.ID.String(), 0)
			return
		}
		c.Header("Content-Type", "text/event-stream; charset=utf-8")
		c.Header("Cache-Control", "no-cache, no-transform")
		c.Header("X-Accel-Buffering", "no")
		c.Status(http.StatusOK)
		if err := writeQuestionEvent(c, "accepted", gin.H{"question_id": question.ID,
			"conversation_id": question.ConversationID, "client_request_key": question.RequestKey,
			"result_id": question.ResultID, "query_url": "/v1/questions/" + question.ID.String(),
			"replayed": true}); err == nil {
			_ = writeQuestionEvent(c, "answer", gin.H{"question_id": question.ID, "answer": question.Answer,
				"replayed": true})
			_ = writeQuestionEvent(c, "done", gin.H{"question_id": question.ID, "status": "SUCCEEDED", "replayed": true})
		}
	case "RUNNING":
		writeError(c, http.StatusConflict, "QUESTION_IN_PROGRESS", "该问题仍在处理中，请查询原问答", true,
			question.ID.String(), 1000)
	case "FAILED", "INTERRUPTED":
		writeError(c, http.StatusConflict, "QUESTION_TERMINAL", "该问答已结束；如需重新生成，请使用新的 Idempotency-Key",
			false, question.ID.String(), 0)
	default:
		writeError(c, http.StatusConflict, "QUESTION_STATE_INVALID", "问答状态暂不可用", false, question.ID.String(), 0)
	}
}

func (s *Server) handleQuestionAdmissionError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, postgres.ErrQuestionNotFound):
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
	case errors.Is(err, postgres.ErrQuestionContentNotReady):
		writeError(c, http.StatusConflict, "CONTENT_NOT_READY", "视频尚无可问答的解析结果", true, "", 0)
	case errors.Is(err, postgres.ErrQuestionInProgress):
		writeError(c, http.StatusTooManyRequests, "USER_QUESTION_LIMIT", "每位用户同时只能处理一个问答", true, "", 1000)
	case errors.Is(err, postgres.ErrQuestionConversation):
		writeError(c, http.StatusNotFound, "CONVERSATION_NOT_FOUND", "对话不存在或已归档", false, "", 0)
	case errors.Is(err, postgres.ErrQuestionIdempotency):
		writeError(c, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "该请求键已用于不同的问题", false, "", 0)
	default:
		var runErr *qa.RunError
		if errors.As(err, &runErr) {
			status := http.StatusBadRequest
			if runErr.Code == "INVALID_IDEMPOTENCY_KEY" {
				status = http.StatusBadRequest
			}
			writeError(c, status, runErr.Code, runErr.Message, runErr.Retryable, "", 0)
			return
		}
		s.logger.Error("question admission failed", "request_id", c.GetString("request_id"), "error", err)
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法受理问答", true, "", 1000)
	}
}

func (s *Server) lookupQuestion(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "问答服务暂不可用", true, "", 1000)
		return
	}
	key := strings.TrimSpace(c.Query("client_request_key"))
	if key == "" || len(key) > 128 {
		writeError(c, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "请求键无效", false, "", 0)
		return
	}
	question, err := s.questions.FindByKey(c.Request.Context(), user.ID, key)
	if err != nil {
		if errors.Is(err, postgres.ErrQuestionNotFound) {
			writeError(c, http.StatusNotFound, "NOT_FOUND", "问答不存在", false, "", 0)
			return
		}
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法查询问答", true, "", 1000)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: question, RequestID: c.GetString("request_id")})
}

func (s *Server) getQuestion(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "问答服务暂不可用", true, "", 1000)
		return
	}
	questionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "问答不存在", false, "", 0)
		return
	}
	question, err := s.questions.Get(c.Request.Context(), user.ID, questionID)
	if err != nil {
		if errors.Is(err, postgres.ErrQuestionNotFound) {
			writeError(c, http.StatusNotFound, "NOT_FOUND", "问答不存在", false, "", 0)
			return
		}
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法查询问答", true, questionID.String(), 1000)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: question, RequestID: c.GetString("request_id")})
}

func (s *Server) cancelQuestion(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.questions == nil {
		writeError(c, http.StatusServiceUnavailable, "QUESTION_SERVICE_DISABLED", "问答服务暂不可用", true, "", 1000)
		return
	}
	questionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "问答不存在", false, "", 0)
		return
	}
	status, err := s.questions.Cancel(c.Request.Context(), user.ID, questionID)
	if err != nil {
		if errors.Is(err, postgres.ErrQuestionNotFound) {
			writeError(c, http.StatusNotFound, "NOT_FOUND", "问答不存在", false, "", 0)
			return
		}
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法取消问答", true, questionID.String(), 1000)
		return
	}
	s.questionCancelMu.Lock()
	cancel := s.questionCancels[questionID]
	s.questionCancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.JSON(http.StatusOK, envelope{Data: gin.H{"question_id": questionID, "execution_status": status},
		RequestID: c.GetString("request_id")})
}

func (s *Server) renewQuestionLease(ctx context.Context, cancel context.CancelFunc, question postgres.Question, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(context.Background(), 2*time.Second)
			renewed, err := s.questions.RenewLease(renewCtx, question)
			renewCancel()
			if err != nil || !renewed {
				cancel()
				return
			}
		}
	}
}

func (s *Server) registerQuestionCancel(id uuid.UUID, cancel context.CancelFunc) bool {
	s.questionCancelMu.Lock()
	defer s.questionCancelMu.Unlock()
	if s.questionsStopping {
		return false
	}
	s.questionCancels[id] = cancel
	return true
}

func (s *Server) questionExecutionsStopping() bool {
	s.questionCancelMu.Lock()
	defer s.questionCancelMu.Unlock()
	return s.questionsStopping
}

func (s *Server) stopQuestionExecutions() {
	s.questionCancelMu.Lock()
	s.questionsStopping = true
	cancels := make([]context.CancelFunc, 0, len(s.questionCancels))
	for _, cancel := range s.questionCancels {
		cancels = append(cancels, cancel)
	}
	s.questionCancelMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (s *Server) unregisterQuestionCancel(id uuid.UUID) {
	s.questionCancelMu.Lock()
	delete(s.questionCancels, id)
	s.questionCancelMu.Unlock()
}

func (s *Server) finishDisconnectedQuestion(question postgres.Question, status, code, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.questions.Finish(ctx, question, status, code, message); err != nil {
		s.logger.Warn("question terminal state will be recovered by lease maintenance",
			"question_id", question.ID, "error", err)
	}
}

func questionFailure(contextErr error, err error) (string, string, string) {
	if errors.Is(contextErr, context.Canceled) {
		return "INTERRUPTED", "EXECUTION_CANCELLED", "问答执行已取消或连接已断开"
	}
	if errors.Is(contextErr, context.DeadlineExceeded) {
		return "FAILED", "QA_TIMEOUT", "问答超过总处理时限"
	}
	var runErr *qa.RunError
	if errors.As(err, &runErr) {
		return "FAILED", runErr.Code, runErr.Message
	}
	if errors.Is(err, postgres.ErrQuestionExecutionLost) {
		return "INTERRUPTED", "EXECUTION_LOST", "问答执行资格已失效"
	}
	return "FAILED", "QUESTION_FAILED", "问答处理失败"
}

func writeQuestionEvent(c *gin.Context, event string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode SSE event: %w", err)
	}
	controller := http.NewResponseController(c.Writer)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c.Writer, "event: "+event+"\ndata: "+string(encoded)+"\n\n"); err != nil {
		return err
	}
	c.Writer.Flush()
	return nil
}

func writeQuestionHeartbeat(c *gin.Context) error {
	controller := http.NewResponseController(c.Writer)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c.Writer, ": keep-alive\n\n"); err != nil {
		return err
	}
	c.Writer.Flush()
	return nil
}
