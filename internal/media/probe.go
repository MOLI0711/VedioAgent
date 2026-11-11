package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

const ProbeProfile = "probe-v2"

var ErrUnsupportedMedia = errors.New("unsupported or invalid media")

type ProbeMetadata struct {
	Profile          string           `json:"profile"`
	Container        string           `json:"container"`
	FormatName       string           `json:"format_name"`
	DurationMS       int64            `json:"duration_ms"`
	VideoCodec       string           `json:"video_codec"`
	VideoStartMS     int64            `json:"video_start_ms"`
	AudioCodec       string           `json:"audio_codec,omitempty"`
	AudioStreamIndex *int             `json:"audio_stream_index,omitempty"`
	AudioOriginMS    int64            `json:"audio_origin_ms"`
	AudioCandidates  []AudioCandidate `json:"audio_candidates,omitempty"`
	HasAudio         bool             `json:"has_audio"`
}

type AudioCandidate struct {
	StreamIndex int    `json:"stream_index"`
	Codec       string `json:"codec"`
	OriginMS    int64  `json:"origin_ms"`
	Default     bool   `json:"default"`
}

type rawAudioCandidate struct {
	index     int
	codec     string
	start     float64
	isDefault bool
}

type ffprobeDocument struct {
	Format struct {
		FormatName string `json:"format_name"`
		Duration   string `json:"duration"`
		StartTime  string `json:"start_time"`
	} `json:"format"`
	Streams []struct {
		Index       int    `json:"index"`
		CodecType   string `json:"codec_type"`
		CodecName   string `json:"codec_name"`
		SampleRate  string `json:"sample_rate"`
		Channels    int    `json:"channels"`
		StartTime   string `json:"start_time"`
		Duration    string `json:"duration"`
		TimeBase    string `json:"time_base"`
		Disposition struct {
			Default int `json:"default"`
		} `json:"disposition"`
	} `json:"streams"`
}

func Probe(ctx context.Context, ffprobePath, inputPath string) (ProbeMetadata, error) {
	output, err := runTool(ctx, ffprobePath, "-v", "error", "-show_streams", "-show_format", "-of", "json", inputPath)
	if err != nil {
		if ctx.Err() != nil {
			return ProbeMetadata{}, err
		}
		return ProbeMetadata{}, fmt.Errorf("%w: %v", ErrUnsupportedMedia, err)
	}
	var document ffprobeDocument
	if err := json.Unmarshal(output, &document); err != nil {
		return ProbeMetadata{}, fmt.Errorf("%w: malformed ffprobe output: %v", ErrUnsupportedMedia, err)
	}
	formatName := strings.ToLower(strings.TrimSpace(document.Format.FormatName))
	if !formatIncludesMP4(formatName) {
		return ProbeMetadata{}, fmt.Errorf("%w: expected MP4 container, got %q", ErrUnsupportedMedia, formatName)
	}
	formatStart := 0.0
	if strings.TrimSpace(document.Format.StartTime) != "" && strings.TrimSpace(document.Format.StartTime) != "N/A" {
		formatStart, err = parseSeconds(document.Format.StartTime)
		if err != nil {
			return ProbeMetadata{}, fmt.Errorf("%w: invalid container start time", ErrUnsupportedMedia)
		}
	}
	videoCount := 0
	videoCodec := ""
	videoStart := formatStart
	var audioCandidates []rawAudioCandidate
	for _, stream := range document.Streams {
		switch stream.CodecType {
		case "video":
			videoCount++
			if videoCount == 1 {
				videoCodec = strings.ToLower(stream.CodecName)
				start, parseErr := parseSeconds(stream.StartTime)
				if parseErr == nil {
					videoStart = start
				}
			}
		case "audio":
			start, parseErr := parseSeconds(stream.StartTime)
			if parseErr != nil {
				start = formatStart
			}
			audioCandidates = append(audioCandidates, rawAudioCandidate{
				index: stream.Index, codec: strings.ToLower(stream.CodecName), start: start,
				isDefault: stream.Disposition.Default == 1,
			})
		}
	}
	if videoCount == 0 || videoCodec != "h264" {
		return ProbeMetadata{}, fmt.Errorf("%w: video stream must use H.264/AVC", ErrUnsupportedMedia)
	}
	durationSeconds, err := parseSeconds(document.Format.Duration)
	if err != nil || durationSeconds <= 0 || durationSeconds > 120*60 {
		return ProbeMetadata{}, fmt.Errorf("%w: duration must be within 120 minutes", ErrUnsupportedMedia)
	}
	metadata := ProbeMetadata{
		Profile: ProbeProfile, Container: "mp4", FormatName: formatName,
		DurationMS: int64(math.Round(durationSeconds * 1000)), VideoCodec: videoCodec,
		VideoStartMS: int64(math.Round(videoStart * 1000)),
	}
	if len(audioCandidates) == 0 {
		return metadata, nil
	}
	usableCandidates := make([]AudioCandidate, 0, len(audioCandidates))
	for _, candidate := range audioCandidates {
		if candidate.codec != "aac" {
			continue
		}
		originMS := int64(math.Round((candidate.start - videoStart) * 1000))
		if originMS > metadata.DurationMS || originMS < -metadata.DurationMS {
			continue
		}
		usableCandidates = append(usableCandidates, AudioCandidate{StreamIndex: candidate.index, Codec: candidate.codec,
			OriginMS: originMS, Default: candidate.isDefault})
	}
	if len(usableCandidates) == 0 {
		return ProbeMetadata{}, fmt.Errorf("%w: audio tracks must use AAC or be absent", ErrUnsupportedMedia)
	}
	sort.SliceStable(usableCandidates, func(i, j int) bool {
		return usableCandidates[i].Default && !usableCandidates[j].Default
	})
	selected := usableCandidates[0]
	metadata.AudioCodec = selected.Codec
	metadata.AudioStreamIndex = &selected.StreamIndex
	metadata.AudioOriginMS = selected.OriginMS
	metadata.AudioCandidates = usableCandidates
	metadata.HasAudio = true
	return metadata, nil
}

func formatIncludesMP4(value string) bool {
	for _, name := range strings.Split(value, ",") {
		switch strings.TrimSpace(name) {
		case "mp4", "mov", "m4a", "3gp", "3g2", "mj2":
			return true
		}
	}
	return false
}

func parseSeconds(value string) (float64, error) {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) == "N/A" {
		return 0, errors.New("missing media time")
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, errors.New("invalid media time")
	}
	return seconds, nil
}
