package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

const (
	AudioProfile    = "pcm-s16le-mono-16khz-v1"
	MaxAudioBytes   = 231 << 20
	MaxAudioTimeSec = 120 * 60
)

var ErrInvalidAudioArtifact = errors.New("invalid extracted audio artifact")

func ExtractAudio(ctx context.Context, ffmpegPath, inputPath, outputPath string, streamIndex int) (int64, error) {
	if streamIndex < 0 {
		return 0, errors.New("audio stream index must be non-negative")
	}
	_, err := runTool(ctx, ffmpegPath,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-y",
		"-i", inputPath, "-map", fmt.Sprintf("0:%d", streamIndex), "-vn",
		"-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le",
		"-t", fmt.Sprintf("%d", MaxAudioTimeSec), "-map_metadata", "-1", "-f", "wav", outputPath)
	if err != nil {
		return 0, err
	}
	stat, err := os.Stat(outputPath)
	if err != nil {
		return 0, fmt.Errorf("stat extracted audio: %w", err)
	}
	if !stat.Mode().IsRegular() || stat.Size() <= 44 || stat.Size() > MaxAudioBytes {
		return 0, fmt.Errorf("extracted audio size is outside allowed bounds: %d", stat.Size())
	}
	return stat.Size(), nil
}

type WAVMetadata struct {
	Codec      string `json:"codec"`
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
	DurationMS int64  `json:"duration_ms"`
	SizeBytes  int64  `json:"size_bytes"`
}

func ValidateWAV(ctx context.Context, ffprobePath, path string) (WAVMetadata, error) {
	output, err := runTool(ctx, ffprobePath, "-v", "error", "-select_streams", "a:0", "-show_streams", "-show_format", "-of", "json", path)
	if err != nil {
		if ctx.Err() != nil {
			return WAVMetadata{}, err
		}
		return WAVMetadata{}, fmt.Errorf("%w: ffprobe failed: %v", ErrInvalidAudioArtifact, err)
	}
	var document ffprobeDocument
	if err := json.Unmarshal(output, &document); err != nil {
		return WAVMetadata{}, fmt.Errorf("%w: malformed ffprobe output: %v", ErrInvalidAudioArtifact, err)
	}
	if !strings.Contains(strings.ToLower(document.Format.FormatName), "wav") || len(document.Streams) != 1 {
		return WAVMetadata{}, fmt.Errorf("%w: expected a single-stream WAV file", ErrInvalidAudioArtifact)
	}
	stream := document.Streams[0]
	rate, err := strconv.Atoi(stream.SampleRate)
	if err != nil || stream.CodecType != "audio" || strings.ToLower(stream.CodecName) != "pcm_s16le" ||
		rate != 16000 || stream.Channels != 1 {
		return WAVMetadata{}, fmt.Errorf("%w: expected 16 kHz mono pcm_s16le", ErrInvalidAudioArtifact)
	}
	duration, err := parseSeconds(document.Format.Duration)
	if err != nil || duration <= 0 || duration > MaxAudioTimeSec || math.IsNaN(duration) || math.IsInf(duration, 0) {
		return WAVMetadata{}, fmt.Errorf("%w: duration is outside allowed bounds", ErrInvalidAudioArtifact)
	}
	stat, err := os.Stat(path)
	if err != nil {
		return WAVMetadata{}, fmt.Errorf("stat validated WAV: %w", err)
	}
	if !stat.Mode().IsRegular() || stat.Size() <= 44 || stat.Size() > MaxAudioBytes {
		return WAVMetadata{}, fmt.Errorf("%w: size is outside allowed bounds", ErrInvalidAudioArtifact)
	}
	return WAVMetadata{Codec: "pcm_s16le", SampleRate: rate, Channels: stream.Channels,
		DurationMS: int64(math.Round(duration * 1000)), SizeBytes: stat.Size()}, nil
}
