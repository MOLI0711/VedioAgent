package localdemo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"mirrorcore/internal/storage/ossstore"
)

const LocalDemoBucket = "local-demo"

// FileObjectStore is an immutable, process-local stand-in for the object subset
// needed by media workers. It is enabled only by LOCAL_DEMO_MODE.
type FileObjectStore struct{ root string }

type objectManifest struct {
	VersionID string            `json:"version_id"`
	Size      int64             `json:"size"`
	SHA256    string            `json:"sha256"`
	Metadata  map[string]string `json:"metadata"`
}

func NewFileObjectStore(root string) (*FileObjectStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("local demo object store requires a root directory")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve local demo object root: %w", err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create local demo object root: %w", err)
	}
	return &FileObjectStore{root: root}, nil
}

func (s *FileObjectStore) objectPath(key string) (string, error) {
	if strings.TrimSpace(key) != key || key == "" || strings.ContainsAny(key, "\\\x00") || strings.HasPrefix(key, "/") {
		return "", errors.New("invalid local object key")
	}
	clean := path.Clean(key)
	if clean != key || clean == "." || strings.HasPrefix(clean, "../") {
		return "", errors.New("invalid local object key")
	}
	for _, part := range strings.Split(clean, "/") {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("invalid local object key")
		}
	}
	full := filepath.Join(s.root, filepath.FromSlash(clean))
	rel, err := filepath.Rel(s.root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("local object key escapes store root")
	}
	return full, nil
}

func (s *FileObjectStore) PresignGet(ctx context.Context, key, versionID string, ttl time.Duration) (ossstore.SignedRequest, error) {
	if err := ctx.Err(); err != nil {
		return ossstore.SignedRequest{}, err
	}
	info, exists, err := s.HeadObjectVersion(ctx, key, versionID)
	if err != nil {
		return ossstore.SignedRequest{}, err
	}
	if !exists || ttl <= 0 {
		return ossstore.SignedRequest{}, errors.New("local object is missing or URL lifetime is invalid")
	}
	expires := time.Now().Add(ttl)
	return ossstore.SignedRequest{Method: "GET", URL: "local-demo://" + LocalDemoBucket + "/" + key + "?version=" + info.VersionID,
		Headers: map[string]string{}, ExpiresAt: expires}, nil
}

func (s *FileObjectStore) HeadObjectVersion(ctx context.Context, key, versionID string) (ossstore.ObjectInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return ossstore.ObjectInfo{}, false, err
	}
	full, err := s.objectPath(key)
	if err != nil {
		return ossstore.ObjectInfo{}, false, err
	}
	manifestBytes, err := os.ReadFile(full + ".meta.json")
	if errors.Is(err, os.ErrNotExist) {
		return ossstore.ObjectInfo{}, false, nil
	}
	if err != nil {
		return ossstore.ObjectInfo{}, false, fmt.Errorf("read local object manifest: %w", err)
	}
	var manifest objectManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil || manifest.VersionID == "" || manifest.Size < 0 {
		return ossstore.ObjectInfo{}, false, errors.New("local object manifest is invalid")
	}
	stat, err := os.Stat(full)
	if errors.Is(err, os.ErrNotExist) {
		return ossstore.ObjectInfo{}, false, nil
	}
	if err != nil {
		return ossstore.ObjectInfo{}, false, fmt.Errorf("stat local object: %w", err)
	}
	if !stat.Mode().IsRegular() || stat.Size() != manifest.Size {
		return ossstore.ObjectInfo{}, false, errors.New("local object does not match its manifest")
	}
	if versionID != "" && versionID != manifest.VersionID {
		return ossstore.ObjectInfo{}, false, nil
	}
	return ossstore.ObjectInfo{Size: manifest.Size, VersionID: manifest.VersionID, Metadata: cloneStringMap(manifest.Metadata)}, true, nil
}

func (s *FileObjectStore) DownloadObject(ctx context.Context, key, versionID string, dst io.Writer, maxBytes int64) (ossstore.ObjectInfo, error) {
	info, exists, err := s.HeadObjectVersion(ctx, key, versionID)
	if err != nil {
		return ossstore.ObjectInfo{}, err
	}
	if !exists {
		return ossstore.ObjectInfo{}, os.ErrNotExist
	}
	if dst == nil || maxBytes <= 0 || info.Size > maxBytes {
		return ossstore.ObjectInfo{}, errors.New("local object download exceeds its configured size bound")
	}
	full, err := s.objectPath(key)
	if err != nil {
		return ossstore.ObjectInfo{}, err
	}
	file, err := os.Open(full)
	if err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("open local object: %w", err)
	}
	defer file.Close()
	written, err := io.Copy(dst, io.LimitReader(&contextReader{ctx: ctx, reader: file}, maxBytes+1))
	if err != nil {
		return ossstore.ObjectInfo{}, err
	}
	if written != info.Size || written > maxBytes {
		return ossstore.ObjectInfo{}, errors.New("local object length changed during download")
	}
	return info, nil
}

func (s *FileObjectStore) PutObjectFile(ctx context.Context, key, sourcePath, _ string, metadata map[string]string) (ossstore.ObjectInfo, error) {
	full, err := s.objectPath(key)
	if err != nil {
		return ossstore.ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("create local object prefix: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("open local object source: %w", err)
	}
	defer source.Close()
	tmp, err := os.CreateTemp(filepath.Dir(full), ".mirrorcore-object-*")
	if err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("create local object staging file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmp, hash), &contextReader{ctx: ctx, reader: source})
	if copyErr == nil {
		copyErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if copyErr != nil || closeErr != nil {
		return ossstore.ObjectInfo{}, errors.Join(copyErr, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return ossstore.ObjectInfo{}, err
	}
	sha := hex.EncodeToString(hash.Sum(nil))
	version := "sha256:" + sha
	manifest := objectManifest{VersionID: version, Size: written, SHA256: sha, Metadata: cloneStringMap(metadata)}
	manifestPath := full + ".meta.json"
	if existing, exists, headErr := s.HeadObjectVersion(ctx, key, ""); headErr != nil {
		return ossstore.ObjectInfo{}, headErr
	} else if exists {
		if existing.VersionID != version || existing.Size != written || !stringMapsEqual(existing.Metadata, manifest.Metadata) {
			return ossstore.ObjectInfo{}, errors.New("local object key is immutable and already contains different content or metadata")
		}
		return existing, nil
	}
	if err := os.Link(tmpName, full); err != nil && !errors.Is(err, os.ErrExist) {
		return ossstore.ObjectInfo{}, fmt.Errorf("commit local object data without overwriting an existing key: %w", err)
	}
	actualSize, actualSHA, err := fingerprintFile(full)
	if err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("verify local object data: %w", err)
	}
	if actualSize != written || actualSHA != sha {
		return ossstore.ObjectInfo{}, errors.New("local object key is immutable and already contains different bytes")
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return ossstore.ObjectInfo{}, err
	}
	metaTmp, err := os.CreateTemp(filepath.Dir(full), ".mirrorcore-manifest-*")
	if err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("create local object manifest staging file: %w", err)
	}
	metaName := metaTmp.Name()
	defer os.Remove(metaName)
	if err := metaTmp.Chmod(0600); err == nil {
		_, err = metaTmp.Write(encoded)
	}
	if err == nil {
		err = metaTmp.Sync()
	}
	if closeErr := metaTmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return ossstore.ObjectInfo{}, fmt.Errorf("write local object manifest: %w", err)
	}
	if err := os.Link(metaName, manifestPath); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return ossstore.ObjectInfo{}, fmt.Errorf("commit local object manifest: %w", err)
		}
		existingBytes, readErr := os.ReadFile(manifestPath)
		var existingManifest objectManifest
		if readErr != nil || json.Unmarshal(existingBytes, &existingManifest) != nil ||
			existingManifest.VersionID != manifest.VersionID || existingManifest.Size != manifest.Size ||
			existingManifest.SHA256 != manifest.SHA256 || !stringMapsEqual(existingManifest.Metadata, manifest.Metadata) {
			return ossstore.ObjectInfo{}, errors.New("local object key already has a different immutable manifest")
		}
	}
	return ossstore.ObjectInfo{Size: written, VersionID: version, Metadata: cloneStringMap(metadata)}, nil
}

func fingerprintFile(path string) (int64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0, "", err
	}
	if !stat.Mode().IsRegular() {
		return 0, "", errors.New("local object path is not a regular file")
	}
	hash := sha256.New()
	count, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", err
	}
	return count, hex.EncodeToString(hash.Sum(nil)), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func cloneStringMap(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}
