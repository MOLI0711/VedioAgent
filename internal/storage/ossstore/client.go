package ossstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"mirrorcore/internal/config"
)

var ErrNotConfigured = errors.New("OSS is not configured")

type ObjectStore interface {
	InitiateMultipart(context.Context, string, string, string) (string, error)
	PresignPart(context.Context, string, string, int32, int64, time.Duration) (SignedRequest, error)
	PresignGet(context.Context, string, string, time.Duration) (SignedRequest, error)
	ListParts(context.Context, string, string) ([]UploadedPart, error)
	CompleteMultipart(context.Context, string, string, []UploadedPart) error
	HeadObject(context.Context, string) (ObjectInfo, bool, error)
	AbortMultipart(context.Context, string, string) error
	HeadObjectVersion(context.Context, string, string) (ObjectInfo, bool, error)
	ListObjectVersions(context.Context, string, string, string) (ObjectVersionPage, error)
	DeleteObject(context.Context, string, string) error
	DownloadObject(context.Context, string, string, io.Writer, int64) (ObjectInfo, error)
	PutObjectFile(context.Context, string, string, string, map[string]string) (ObjectInfo, error)
}

type SignedRequest struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

type UploadedPart struct {
	Number int32
	Size   int64
	ETag   string
}

type ObjectInfo struct {
	Size      int64
	VersionID string
	Metadata  map[string]string
}

type ListedObjectVersion struct {
	Key       string
	VersionID string
}

type ObjectVersionPage struct {
	Objects             []ListedObjectVersion
	IsTruncated         bool
	NextKeyMarker       string
	NextVersionIDMarker string
}

type Client struct {
	client      *oss.Client
	bucket      string
	credentials credentials.CredentialsProvider
}

func New(cfg config.ObjectStorage) (*Client, error) {
	if !cfg.Enabled() {
		return nil, ErrNotConfigured
	}
	var provider credentials.CredentialsProvider
	switch cfg.CredentialMode {
	case "env":
		provider = credentials.NewEnvironmentVariableCredentialsProvider()
	case "ecs":
		provider = credentials.NewEcsRoleCredentialsProvider()
	default:
		return nil, fmt.Errorf("unsupported OSS credential mode %q", cfg.CredentialMode)
	}
	sdkConfig := oss.LoadDefaultConfig().
		WithRegion(cfg.Region).
		WithCredentialsProvider(provider).
		WithRetryMaxAttempts(1).
		WithConnectTimeout(5 * time.Second).
		WithReadWriteTimeout(30 * time.Second).
		WithMaxConnections(32)
	if cfg.Endpoint != "" {
		sdkConfig.WithEndpoint(cfg.Endpoint)
	}
	return &Client{client: oss.NewClient(sdkConfig), bucket: cfg.Bucket, credentials: provider}, nil
}

func (c *Client) InitiateMultipart(ctx context.Context, key, sessionID, videoID string) (string, error) {
	result, err := c.client.InitiateMultipartUpload(ctx, &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key),
		ContentType: oss.Ptr("video/mp4"), ForbidOverwrite: oss.Ptr("true"),
		Metadata: map[string]string{"upload-session-id": sessionID, "video-id": videoID},
	})
	if err != nil {
		return "", fmt.Errorf("initiate OSS multipart upload: %w", err)
	}
	if result.UploadId == nil || *result.UploadId == "" {
		return "", errors.New("OSS returned an empty multipart upload ID")
	}
	return *result.UploadId, nil
}

func (c *Client) PresignPart(ctx context.Context, key, uploadID string, partNumber int32, contentLength int64, ttl time.Duration) (SignedRequest, error) {
	result, err := c.client.Presign(ctx, &oss.UploadPartRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key), UploadId: oss.Ptr(uploadID), PartNumber: partNumber,
		ContentLength: oss.Ptr(contentLength),
	}, func(options *oss.PresignOptions) { options.Expires = ttl })
	if err != nil {
		return SignedRequest{}, fmt.Errorf("presign OSS upload part: %w", err)
	}
	return SignedRequest{Method: result.Method, URL: result.URL, Headers: result.SignedHeaders, ExpiresAt: result.Expiration}, nil
}

// PresignGet signs a version-pinned audio GET. Temporary credentials must outlive
// the complete signed URL window plus the caller's safety margin.
func (c *Client) PresignGet(ctx context.Context, key, versionID string, ttl time.Duration) (SignedRequest, error) {
	if ttl <= 0 {
		return SignedRequest{}, errors.New("OSS presigned GET lifetime must be positive")
	}
	credentials, err := c.credentials.GetCredentials(ctx)
	if err != nil {
		return SignedRequest{}, fmt.Errorf("load OSS credentials for audio URL: %w", err)
	}
	result, err := c.client.Presign(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key), VersionId: optionalString(versionID),
	}, func(options *oss.PresignOptions) { options.Expires = ttl })
	if err != nil {
		return SignedRequest{}, fmt.Errorf("presign OSS audio GET: %w", err)
	}
	if credentials.Expires != nil && credentials.Expires.Before(result.Expiration.Add(30*time.Minute)) {
		return SignedRequest{}, errors.New("OSS credentials expire before the ASR audio URL safety window")
	}
	return SignedRequest{Method: result.Method, URL: result.URL, Headers: result.SignedHeaders, ExpiresAt: result.Expiration}, nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return oss.Ptr(value)
}

func (c *Client) ListParts(ctx context.Context, key, uploadID string) ([]UploadedPart, error) {
	paginator := c.client.NewListPartsPaginator(&oss.ListPartsRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key), UploadId: oss.Ptr(uploadID), MaxParts: 1000,
	})
	parts := make([]UploadedPart, 0)
	for paginator.HasNext() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list OSS multipart parts: %w", err)
		}
		for _, part := range page.Parts {
			item := UploadedPart{Number: part.PartNumber, Size: part.Size}
			if part.ETag != nil {
				item.ETag = *part.ETag
			}
			parts = append(parts, item)
		}
	}
	return parts, nil
}

func (c *Client) CompleteMultipart(ctx context.Context, key, uploadID string, parts []UploadedPart) error {
	ossParts := make([]oss.UploadPart, len(parts))
	for i, part := range parts {
		ossParts[i] = oss.UploadPart{PartNumber: part.Number, ETag: oss.Ptr(part.ETag)}
	}
	_, err := c.client.CompleteMultipartUpload(ctx, &oss.CompleteMultipartUploadRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key), UploadId: oss.Ptr(uploadID),
		ForbidOverwrite:         oss.Ptr("true"),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{Parts: ossParts},
	})
	if err != nil {
		return fmt.Errorf("complete OSS multipart upload: %w", err)
	}
	return nil
}

func (c *Client) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	return c.HeadObjectVersion(ctx, key, "")
}

func (c *Client) HeadObjectVersion(ctx context.Context, key, versionID string) (ObjectInfo, bool, error) {
	request := &oss.HeadObjectRequest{Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key)}
	if versionID != "" {
		request.VersionId = oss.Ptr(versionID)
	}
	result, err := c.client.HeadObject(ctx, request)
	if err != nil {
		var serviceErr *oss.ServiceError
		if errors.As(err, &serviceErr) && serviceErr.HttpStatusCode() == 404 {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, fmt.Errorf("head OSS object: %w", err)
	}
	info := ObjectInfo{Size: result.ContentLength, Metadata: result.Metadata}
	if result.VersionId != nil {
		info.VersionID = *result.VersionId
	}
	return info, true, nil
}

// ListObjectVersions returns one bounded page, including delete markers, so a
// deletion job can persist its cursor and remove old versions as well as the
// current object. The caller supplies a prefix generated from a video ID.
func (c *Client) ListObjectVersions(ctx context.Context, prefix, keyMarker, versionMarker string) (ObjectVersionPage, error) {
	if prefix == "" {
		return ObjectVersionPage{}, errors.New("OSS object version prefix is required")
	}
	result, err := c.client.ListObjectVersions(ctx, &oss.ListObjectVersionsRequest{
		Bucket: oss.Ptr(c.bucket), Prefix: oss.Ptr(prefix), MaxKeys: 500,
		KeyMarker: optionalString(keyMarker), VersionIdMarker: optionalString(versionMarker),
		EncodingType: oss.Ptr("url"),
	})
	if err != nil {
		return ObjectVersionPage{}, fmt.Errorf("list OSS object versions: %w", err)
	}
	page := ObjectVersionPage{IsTruncated: result.IsTruncated}
	if result.NextKeyMarker != nil {
		page.NextKeyMarker = *result.NextKeyMarker
	}
	if result.NextVersionIdMarker != nil {
		page.NextVersionIDMarker = *result.NextVersionIdMarker
	}
	for _, object := range result.ObjectVersions {
		if object.Key == nil || *object.Key == "" {
			return ObjectVersionPage{}, errors.New("OSS returned an object version without a key")
		}
		item := ListedObjectVersion{Key: *object.Key}
		if object.VersionId != nil {
			item.VersionID = *object.VersionId
		}
		page.Objects = append(page.Objects, item)
	}
	for _, marker := range result.ObjectDeleteMarkers {
		if marker.Key == nil || *marker.Key == "" {
			return ObjectVersionPage{}, errors.New("OSS returned a delete marker without a key")
		}
		item := ListedObjectVersion{Key: *marker.Key}
		if marker.VersionId != nil {
			item.VersionID = *marker.VersionId
		}
		page.Objects = append(page.Objects, item)
	}
	if page.IsTruncated && page.NextKeyMarker == "" {
		return ObjectVersionPage{}, errors.New("OSS returned a truncated object version page without a usable cursor")
	}
	return page, nil
}

// DeleteObject removes the recorded object version. A missing object is already
// in the desired state, which makes retry after an ambiguous response safe.
func (c *Client) DeleteObject(ctx context.Context, key, versionID string) error {
	request := &oss.DeleteObjectRequest{Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key)}
	if versionID != "" {
		request.VersionId = oss.Ptr(versionID)
	}
	_, err := c.client.DeleteObject(ctx, request)
	if err != nil {
		var serviceErr *oss.ServiceError
		if errors.As(err, &serviceErr) && serviceErr.HttpStatusCode() == 404 {
			return nil
		}
		return fmt.Errorf("delete OSS object: %w", err)
	}
	return nil
}

func (c *Client) AbortMultipart(ctx context.Context, key, uploadID string) error {
	_, err := c.client.AbortMultipartUpload(ctx, &oss.AbortMultipartUploadRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key), UploadId: oss.Ptr(uploadID),
	})
	if err != nil {
		var serviceErr *oss.ServiceError
		if errors.As(err, &serviceErr) && serviceErr.HttpStatusCode() == 404 {
			// The request may have succeeded before the caller lost its response.
			return nil
		}
		return fmt.Errorf("abort OSS multipart upload: %w", err)
	}
	return nil
}

func (c *Client) DownloadObject(ctx context.Context, key, versionID string, destination io.Writer, maxBytes int64) (ObjectInfo, error) {
	if maxBytes <= 0 {
		return ObjectInfo{}, errors.New("maximum download size must be positive")
	}
	request := &oss.GetObjectRequest{Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key)}
	if versionID != "" {
		request.VersionId = oss.Ptr(versionID)
	}
	result, err := c.client.GetObject(ctx, request)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("get OSS object: %w", err)
	}
	defer func() { _ = result.Body.Close() }()
	written, err := io.Copy(destination, io.LimitReader(result.Body, maxBytes+1))
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("stream OSS object: %w", err)
	}
	if written > maxBytes {
		return ObjectInfo{}, fmt.Errorf("OSS object exceeds maximum download size %d", maxBytes)
	}
	if result.ContentLength >= 0 && written != result.ContentLength {
		return ObjectInfo{}, fmt.Errorf("OSS object length mismatch: header=%d downloaded=%d", result.ContentLength, written)
	}
	info := ObjectInfo{Size: written, Metadata: result.Metadata}
	if result.VersionId != nil {
		info.VersionID = *result.VersionId
	}
	return info, nil
}

func (c *Client) PutObjectFile(ctx context.Context, key, path, contentType string, metadata map[string]string) (ObjectInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("open local object for OSS upload: %w", err)
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("stat local object for OSS upload: %w", err)
	}
	if !stat.Mode().IsRegular() || stat.Size() <= 0 {
		return ObjectInfo{}, errors.New("OSS upload source must be a non-empty regular file")
	}
	result, err := c.client.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key), Body: file,
		ContentLength: oss.Ptr(stat.Size()), ContentType: oss.Ptr(contentType),
		ForbidOverwrite: oss.Ptr("true"), Metadata: metadata,
	})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("put OSS object: %w", err)
	}
	info := ObjectInfo{Size: stat.Size(), Metadata: metadata}
	if result.VersionId != nil {
		info.VersionID = *result.VersionId
	}
	return info, nil
}

func (c *Client) Bucket() string { return c.bucket }
