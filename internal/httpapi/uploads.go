package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"mirrorcore/internal/application"
)

type createUploadRequest struct {
	FileName    string `json:"file_name"`
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
}

func (s *Server) createUpload(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	if s.uploads == nil {
		writeError(c, http.StatusServiceUnavailable, "STORAGE_NOT_CONFIGURED", "视频存储尚未配置", false, "", 0)
		return
	}
	var body createUploadRequest
	if err := decodeJSON(c, &body); err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请求格式不正确", false, "", 0)
		return
	}
	session, replay, err := s.uploads.Create(c.Request.Context(), application.UploadInput{
		OwnerID: user.ID, RequestKey: c.GetHeader("Idempotency-Key"), SourceName: body.FileName,
		SizeBytes: body.SizeBytes, ContentType: body.ContentType,
	}, c.GetString("request_id"))
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	if replay {
		c.Header("Idempotency-Replayed", "true")
	}
	status := http.StatusCreated
	if session.Status == "INITIATING" {
		status = http.StatusAccepted
	}
	c.JSON(status, envelope{Data: session, RequestID: c.GetString("request_id")})
}

func (s *Server) getUpload(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	uploads, ok := s.uploadsReady(c)
	if !ok {
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "上传会话不存在", false, "", 0)
		return
	}
	progress, err := uploads.Get(c.Request.Context(), user.ID, sessionID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: progress, RequestID: c.GetString("request_id")})
}

func (s *Server) lookupUpload(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	uploads, ok := s.uploadsReady(c)
	if !ok {
		return
	}
	progress, err := uploads.Lookup(c.Request.Context(), user.ID, c.Query("client_request_key"))
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: progress, RequestID: c.GetString("request_id")})
}

func (s *Server) createPartURLs(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	uploads, ok := s.uploadsReady(c)
	if !ok {
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "上传会话不存在", false, "", 0)
		return
	}
	var body application.PartURLInput
	if err := decodeJSON(c, &body); err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请求格式不正确", false, "", 0)
		return
	}
	parts, err := uploads.PartURLs(c.Request.Context(), user.ID, sessionID, body)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusOK, envelope{Data: gin.H{"parts": parts}, RequestID: c.GetString("request_id")})
}

func (s *Server) completeUpload(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	uploads, ok := s.uploadsReady(c)
	if !ok {
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "上传会话不存在", false, "", 0)
		return
	}
	var body application.CompleteUploadInput
	if c.Request.ContentLength != 0 {
		if err := decodeJSONLimit(c, &body, 64<<10); err != nil {
			writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "请求格式不正确，请提交分片ETag", false, "", 0)
			return
		}
	}
	result, err := uploads.Complete(c.Request.Context(), user.ID, sessionID, body)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	status := http.StatusAccepted
	if result.Status == "COMPLETE" {
		status = http.StatusOK
	}
	c.JSON(status, envelope{Data: result, RequestID: c.GetString("request_id")})
}

func (s *Server) cancelUpload(c *gin.Context) {
	user, ok := UserFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "请先登录", false, "", 0)
		return
	}
	uploads, ok := s.uploadsReady(c)
	if !ok {
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "上传会话不存在", false, "", 0)
		return
	}
	session, err := uploads.Cancel(c.Request.Context(), user.ID, sessionID)
	if err != nil {
		s.handleResourceError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, envelope{Data: gin.H{
		"upload_session_id": session.ID,
		"video_id":          session.VideoID,
		"status":            session.Status,
	}, RequestID: c.GetString("request_id")})
}

func (s *Server) uploadsReady(c *gin.Context) (*application.UploadService, bool) {
	if s.uploads == nil {
		writeError(c, http.StatusServiceUnavailable, "STORAGE_NOT_CONFIGURED", "视频存储尚未配置", false, "", 0)
		return nil, false
	}
	return s.uploads, true
}
