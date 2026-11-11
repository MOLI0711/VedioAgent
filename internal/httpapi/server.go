package httpapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/auth"
	"mirrorcore/internal/content"
	"mirrorcore/internal/observability"
	"mirrorcore/internal/qa"
	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

type Server struct {
	auth              *auth.Service
	pool              *pgxpool.Pool
	logger            *slog.Logger
	metrics           *observability.Metrics
	cookieSecure      bool
	resources         *application.ResourceService
	uploads           *application.UploadService
	questions         *qa.Service
	contents          *content.Service
	deletions         *postgres.VideoDeletionRepository
	playback          *application.PlaybackService
	questionSlots     chan struct{}
	questionCancelMu  sync.Mutex
	questionCancels   map[uuid.UUID]context.CancelFunc
	questionsStopping bool
}

// Runtime exposes lifecycle controls that are needed by the process owner while
// keeping the Gin engine behind the standard http.Handler interface.
type Runtime struct {
	engine *gin.Engine
	server *Server
}

func (r *Runtime) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.engine.ServeHTTP(w, req)
}

// StopQuestionExecutions rejects new question runs and cancels active ones.
func (r *Runtime) StopQuestionExecutions() {
	r.server.stopQuestionExecutions()
}

type envelope struct {
	Data      any    `json:"data,omitempty"`
	RequestID string `json:"request_id"`
}

type errorEnvelope struct {
	Error     apiError `json:"error"`
	RequestID string   `json:"request_id"`
}

type apiError struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Retryable    bool   `json:"retryable"`
	ResourceID   string `json:"resource_id,omitempty"`
	QueryURL     string `json:"query_url,omitempty"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

//go:embed openapi.yaml
var openAPIDocument []byte

type contextKey string

const userContextKey contextKey = "mirrorcore_user"

func New(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool) *gin.Engine {
	return NewWithServices(pool, logger, cookieSecure, nil, nil)
}

func NewWithUploads(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool, uploads *application.UploadService) *gin.Engine {
	return NewWithServices(pool, logger, cookieSecure, uploads, nil)
}

func NewWithServices(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool, uploads *application.UploadService,
	questions *qa.Service, contentServices ...*content.Service) *gin.Engine {
	return newWithRuntime(pool, logger, cookieSecure, uploads, questions, nil, "", nil, contentServices...).engine
}

func NewWithPlayback(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool, uploads *application.UploadService,
	questions *qa.Service, store ossstore.ObjectStore, bucket string, contentServices ...*content.Service) *gin.Engine {
	return newWithRuntime(pool, logger, cookieSecure, uploads, questions, store, bucket, nil, contentServices...).engine
}

func NewWithPlaybackAndMetrics(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool,
	uploads *application.UploadService, questions *qa.Service, store ossstore.ObjectStore, bucket string,
	metrics *observability.Metrics, contentServices ...*content.Service) *gin.Engine {
	return NewRuntimeWithPlaybackAndMetrics(pool, logger, cookieSecure, uploads, questions, store, bucket, metrics,
		contentServices...).engine
}

func NewRuntimeWithPlaybackAndMetrics(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool,
	uploads *application.UploadService, questions *qa.Service, store ossstore.ObjectStore, bucket string,
	metrics *observability.Metrics, contentServices ...*content.Service) *Runtime {
	return newWithRuntime(pool, logger, cookieSecure, uploads, questions, store, bucket, metrics, contentServices...)
}

func newWithRuntime(pool *pgxpool.Pool, logger *slog.Logger, cookieSecure bool, uploads *application.UploadService,
	questions *qa.Service, store ossstore.ObjectStore, bucket string, metrics *observability.Metrics,
	contentServices ...*content.Service) *Runtime {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = observability.New("api")
	}
	metrics.AttachPool(pool)
	gin.SetMode(gin.ReleaseMode)
	var contents *content.Service
	if len(contentServices) > 0 {
		contents = contentServices[0]
	}
	service := auth.NewService(pool)
	repository := postgres.NewResourceRepository(pool)
	s := &Server{auth: service, pool: pool, logger: logger, metrics: metrics, cookieSecure: cookieSecure,
		resources: application.NewResourceService(repository), uploads: uploads, questions: questions, contents: contents,
		deletions:     postgres.NewVideoDeletionRepository(pool),
		playback:      application.NewPlaybackService(postgres.NewPlaybackRepository(pool), store, bucket),
		questionSlots: make(chan struct{}, 2), questionCancels: make(map[uuid.UUID]context.CancelFunc)}
	router := gin.New()
	router.Use(s.requestID(), s.accessLog(), s.httpMetrics(), gin.Recovery())
	router.GET("/healthz", s.health)
	router.GET("/readyz", s.ready)
	router.GET("/openapi.yaml", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/yaml; charset=utf-8", openAPIDocument)
	})
	router.POST("/v1/sessions", s.createSession)
	router.DELETE("/v1/session", s.deleteSession)
	secure := router.Group("/v1")
	secure.Use(RequireSession(service))
	secure.GET("/videos", s.listVideos)
	secure.GET("/videos/:id", s.getVideo)
	secure.DELETE("/videos/:id", RequireCSRF(), s.deleteVideo)
	secure.GET("/video-deletions/:id", s.getVideoDeletion)
	secure.POST("/videos/:id/parse-tasks", RequireCSRF(), s.createParseTask)
	secure.GET("/parse-tasks/lookup", s.lookupParseTask)
	secure.POST("/parse-tasks/:id/retry", RequireCSRF(), s.retryParseTask)
	secure.POST("/parse-tasks/:id/cancel", RequireCSRF(), s.cancelParseTask)
	secure.POST("/uploads", RequireCSRF(), s.createUpload)
	secure.GET("/uploads/lookup", s.lookupUpload)
	secure.GET("/uploads/:id", s.getUpload)
	secure.POST("/uploads/:id/part-urls", RequireCSRF(), s.createPartURLs)
	secure.POST("/uploads/:id/complete", RequireCSRF(), s.completeUpload)
	secure.DELETE("/uploads/:id", RequireCSRF(), s.cancelUpload)
	secure.GET("/parse-tasks/:id", s.getParseTask)
	secure.GET("/results/:id/content", s.getResultContent)
	secure.POST("/results/:id/content-jobs", RequireCSRF(), s.createContentJob)
	secure.GET("/content-jobs/lookup", s.lookupContentJob)
	secure.GET("/content-jobs/:id", s.getContentJob)
	secure.GET("/content-artifacts/:id", s.getContentArtifact)
	secure.GET("/questions/lookup", s.lookupQuestion)
	secure.GET("/questions/:id", s.getQuestion)
	secure.GET("/videos/:id/playback", s.playVideo)
	secure.POST("/questions/:id/cancel", RequireCSRF(), s.cancelQuestion)
	secure.GET("/conversations/lookup", s.lookupConversation)
	secure.GET("/conversations/:id/questions", s.listConversationQuestions)
	secure.DELETE("/conversations/:id", RequireCSRF(), s.archiveConversation)
	secure.POST("/videos/:id/conversations", RequireCSRF(), s.createConversation)
	secure.GET("/videos/:id/conversations", s.listConversations)
	secure.POST("/videos/:id/questions", RequireCSRF(), s.askQuestion)
	return &Runtime{engine: router, server: s}
}

func (s *Server) httpMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		s.metrics.ObserveHTTPRequest(c.Request.Method, route, c.Writer.Status(), time.Since(started))
	}
}

func (s *Server) Bootstrap(ctx context.Context, email, password string) error {
	return s.auth.SeedDemoUser(ctx, email, password)
}

func (s *Server) requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if _, err := uuid.Parse(id); err != nil {
			id = uuid.NewString()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

func (s *Server) accessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		s.logger.Info("http request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(started).Milliseconds(),
			"client_ip", c.ClientIP(),
		)
	}
}

func (s *Server) health(c *gin.Context) {
	c.JSON(http.StatusOK, envelope{Data: gin.H{"status": "ok"}, RequestID: c.GetString("request_id")})
}

func (s *Server) ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		writeError(c, http.StatusServiceUnavailable, "NOT_READY", "服务暂不可用", true, "", 1000)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: gin.H{"status": "ready", "dependencies": gin.H{"postgres": "ok"}}, RequestID: c.GetString("request_id")})
}

func (s *Server) listVideos(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	limit := 20
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(c, http.StatusBadRequest, "INVALID_LIMIT", "limit 必须在1到100之间", false, "", 0)
			return
		}
		limit = parsed
	}
	var cursorAt *time.Time
	var cursorID *uuid.UUID
	if raw := c.Query("cursor"); raw != "" {
		at, id, err := application.ParseCursor(raw)
		if err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_CURSOR", "分页游标无效", false, "", 0)
			return
		}
		cursorAt, cursorID = &at, &id
	}
	page, err := s.resources.ListVideos(c.Request.Context(), user.ID, limit, cursorAt, cursorID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: page, RequestID: c.GetString("request_id")})
}

func (s *Server) getVideo(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	video, err := s.resources.GetVideo(c.Request.Context(), user.ID, videoID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: video, RequestID: c.GetString("request_id")})
}

func (s *Server) deleteVideo(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	deletion, err := s.resources.DeleteVideo(c.Request.Context(), user.ID, videoID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	status := http.StatusAccepted
	if deletion.State == "SUCCEEDED" {
		status = http.StatusOK
	}
	c.JSON(status, envelope{Data: deletion, RequestID: c.GetString("request_id")})
}

func (s *Server) getVideoDeletion(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	deletionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "删除任务不存在", false, "", 0)
		return
	}
	deletion, err := s.deletions.Get(c.Request.Context(), user.ID, deletionID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: deletion, RequestID: c.GetString("request_id")})
}

func (s *Server) playVideo(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	var questionID *uuid.UUID
	if raw := c.Query("question_id"); raw != "" {
		parsed, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			writeError(c, http.StatusBadRequest, "INVALID_PLAYBACK_REFERENCE", "question_id格式不正确", false, "", 0)
			return
		}
		questionID = &parsed
	}
	var contentArtifactID *uuid.UUID
	if raw := c.Query("content_artifact_id"); raw != "" {
		parsed, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			writeError(c, http.StatusBadRequest, "INVALID_PLAYBACK_REFERENCE", "content_artifact_id格式不正确", false, "", 0)
			return
		}
		contentArtifactID = &parsed
	}
	result, err := s.playback.ForVideo(c.Request.Context(), user.ID, videoID, questionID, c.Query("evidence_id"),
		contentArtifactID, c.Query("content_ref_id"))
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: result, RequestID: c.GetString("request_id")})
}

func (s *Server) createParseTask(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	videoID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "视频不存在", false, "", 0)
		return
	}
	var body struct {
		ProfileID     string `json:"profile_id"`
		ReuseMode     string `json:"reuse_mode"`
		RecomputeFrom string `json:"recompute_from"`
	}
	if c.Request.ContentLength != 0 {
		if err := decodeJSON(c, &body); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请求格式不正确", false, "", 0)
			return
		}
	}
	task, replay, err := s.resources.CreateParseTask(c.Request.Context(), application.CreateParseTaskInput{
		VideoID: videoID, OwnerID: user.ID, RequestKey: c.GetHeader("Idempotency-Key"),
		TraceID: c.GetString("request_id"), ProfileID: body.ProfileID, ReuseMode: body.ReuseMode, RecomputeFrom: body.RecomputeFrom,
	})
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	status := http.StatusAccepted
	if replay {
		c.Header("Idempotency-Replayed", "true")
	}
	c.JSON(status, envelope{Data: task, RequestID: c.GetString("request_id")})
}

func (s *Server) getParseTask(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "解析任务不存在", false, "", 0)
		return
	}
	task, err := s.resources.GetParseTask(c.Request.Context(), user.ID, taskID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: task, RequestID: c.GetString("request_id")})
}

func (s *Server) lookupParseTask(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	task, err := s.resources.LookupParseTask(c.Request.Context(), user.ID, c.Query("client_request_key"))
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: task, RequestID: c.GetString("request_id")})
}

func (s *Server) retryParseTask(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "解析任务不存在", false, "", 0)
		return
	}
	task, replayed, err := s.resources.RetryParseTask(c.Request.Context(), user.ID, taskID, c.GetHeader("Idempotency-Key"))
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		c.Header("Idempotency-Replayed", "true")
	}
	c.JSON(status, envelope{Data: task, RequestID: c.GetString("request_id")})
}

func (s *Server) cancelParseTask(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	taskID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "解析任务不存在", false, "", 0)
		return
	}
	task, changed, err := s.resources.CancelParseTask(c.Request.Context(), user.ID, taskID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	status := http.StatusAccepted
	if !changed {
		status = http.StatusOK
	}
	c.JSON(status, envelope{Data: gin.H{"task": task, "cancel_requested": task.Status == "CANCELLED"},
		RequestID: c.GetString("request_id")})
}

func (s *Server) handleResourceError(c *gin.Context, err error) {
	var serviceErr *application.ServiceError
	if errors.As(err, &serviceErr) {
		writeError(c, serviceErr.Status, serviceErr.Code, serviceErr.Message, serviceErr.Retryable, "", serviceErr.RetryAfterMS)
		return
	}
	s.logger.Error("resource request failed", "request_id", c.GetString("request_id"), "error", err)
	writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法处理请求，请稍后重试", true, "", 1000)
}

func (s *Server) createSession(c *gin.Context) {
	var request loginRequest
	if err := decodeJSON(c, &request); err != nil || strings.TrimSpace(request.Email) == "" || request.Password == "" {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请检查登录信息", false, "", 0)
		return
	}
	result, err := s.auth.Login(c.Request.Context(), request.Email, request.Password)
	if errors.Is(err, auth.ErrInvalidCredentials) {
		writeError(c, http.StatusUnauthorized, "INVALID_CREDENTIALS", "邮箱或密码不正确", false, "", 0)
		return
	}
	if err != nil {
		s.logger.Error("create session failed", "request_id", c.GetString("request_id"), "error", err)
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法登录，请稍后重试", true, "", 1000)
		return
	}
	maxAge := int(time.Until(result.ExpiresAt).Seconds())
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     auth.SessionCookieName,
		Value:    result.Token,
		Path:     "/",
		Expires:  result.ExpiresAt,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   s.cookieSecure,
		SameSite: http.SameSiteStrictMode,
	})
	c.JSON(http.StatusCreated, envelope{
		Data: gin.H{
			"user":       gin.H{"id": result.User.ID, "email": result.User.Email},
			"csrf_token": result.CSRFToken,
			"expires_at": result.ExpiresAt.Format(time.RFC3339Nano),
		},
		RequestID: c.GetString("request_id"),
	})
}

func (s *Server) deleteSession(c *gin.Context) {
	token := cookieValue(c, auth.SessionCookieName)
	err := s.auth.Logout(c.Request.Context(), token, c.GetHeader("X-CSRF-Token"))
	if errors.Is(err, auth.ErrCSRF) {
		writeError(c, http.StatusForbidden, "CSRF_TOKEN_INVALID", "请求校验失败，请刷新后重试", false, "", 0)
		return
	}
	if err != nil {
		s.logger.Error("revoke session failed", "request_id", c.GetString("request_id"), "error", err)
		writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法注销，请稍后重试", true, "", 1000)
		return
	}
	http.SetCookie(c.Writer, &http.Cookie{
		Name: auth.SessionCookieName, Value: "", Path: "/", MaxAge: -1,
		Expires: time.Unix(1, 0), HttpOnly: true, Secure: s.cookieSecure,
		SameSite: http.SameSiteStrictMode,
	})
	c.JSON(http.StatusOK, envelope{Data: gin.H{"logged_out": true}, RequestID: c.GetString("request_id")})
}

// RequireSession and RequireCSRF are applied to resource routes as they are added.
func RequireSession(service *auth.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		session, err := service.Authenticate(c.Request.Context(), cookieValue(c, auth.SessionCookieName))
		if errors.Is(err, auth.ErrUnauthenticated) {
			writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
			c.Abort()
			return
		}
		if err != nil {
			writeError(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "暂时无法验证登录状态", true, "", 1000)
			c.Abort()
			return
		}
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), userContextKey, session.User))
		c.Set("csrf_hash", session.CSRFHash)
		c.Next()
	}
}

func RequireCSRF() gin.HandlerFunc {
	return func(c *gin.Context) {
		storedHash, _ := c.Get("csrf_hash")
		csrfHash, _ := storedHash.([]byte)
		if !auth.VerifyCSRF(csrfHash, c.GetHeader("X-CSRF-Token")) {
			writeError(c, http.StatusForbidden, "CSRF_TOKEN_INVALID", "请求校验失败，请刷新后重试", false, "", 0)
			c.Abort()
			return
		}
		c.Next()
	}
}

func UserFromContext(ctx context.Context) (auth.User, bool) {
	user, ok := ctx.Value(userContextKey).(auth.User)
	return user, ok
}

func cookieValue(c *gin.Context, name string) string {
	cookie, err := c.Request.Cookie(name)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func decodeJSON(c *gin.Context, destination any) error {
	return decodeJSONLimit(c, destination, 16<<10)
}

func decodeJSONLimit(c *gin.Context, destination any, maxBytes int64) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain one JSON value")
		}
		return err
	}
	return nil
}

func writeError(c *gin.Context, status int, code, message string, retryable bool, resourceID string, retryAfterMS int64) {
	if status == http.StatusTooManyRequests && retryAfterMS > 0 {
		seconds := (retryAfterMS + 999) / 1000
		c.Header("Retry-After", strconv.FormatInt(seconds, 10))
	}
	c.JSON(status, errorEnvelope{
		Error:     apiError{Code: code, Message: message, Retryable: retryable, ResourceID: resourceID, RetryAfterMS: retryAfterMS},
		RequestID: c.GetString("request_id"),
	})
}
