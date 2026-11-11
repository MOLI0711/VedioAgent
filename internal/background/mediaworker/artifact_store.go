package mediaworker

import (
	"context"
	"io"
	"time"

	"mirrorcore/internal/storage/ossstore"
)

// ArtifactStore is the immutable object subset used by media processing.
// The production OSS client and the LOCAL_DEMO_MODE filesystem adapter both
// implement it; upload-session operations remain production OSS only.
type ArtifactStore interface {
	PresignGet(context.Context, string, string, time.Duration) (ossstore.SignedRequest, error)
	HeadObjectVersion(context.Context, string, string) (ossstore.ObjectInfo, bool, error)
	DownloadObject(context.Context, string, string, io.Writer, int64) (ossstore.ObjectInfo, error)
	PutObjectFile(context.Context, string, string, string, map[string]string) (ossstore.ObjectInfo, error)
}
