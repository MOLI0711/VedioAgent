package chunk

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"mirrorcore/internal/repository/postgres"
)

const (
	ProfileVersion     = "chunk_v1_bytes"
	DefaultTargetBytes = 512
	DefaultMaxBytes    = 768
	DefaultOverlapMax  = 128
	DefaultOverlapUnit = 1
	MaxSourceSegments  = 1_000_000
)

type Config struct {
	TargetBytes      int `json:"target_bytes"`
	MaxBytes         int `json:"max_bytes"`
	OverlapSentences int `json:"overlap_sentences"`
	MaxOverlapBytes  int `json:"max_overlap_bytes"`
}

func DefaultConfig() Config {
	return Config{TargetBytes: DefaultTargetBytes, MaxBytes: DefaultMaxBytes,
		OverlapSentences: DefaultOverlapUnit, MaxOverlapBytes: DefaultOverlapMax}
}

type Source struct {
	TranscriptSegmentSeq int
	StartByte            int
	EndByte              int
}

type Chunk struct {
	Seq       int
	Text      string
	ByteCount int
	StartMS   int64
	EndMS     int64
	Sources   []Source
	members   []unit
}

type unit struct {
	segmentSeq       int
	startByte        int
	endByte          int
	text             string
	byteCount        int
	completeSentence bool
	startMS          int64
	endMS            int64
}

func (c Config) Validate() error {
	if c.TargetBytes <= 0 || c.MaxBytes < c.TargetBytes || c.OverlapSentences < 0 ||
		c.OverlapSentences > 1 || c.MaxOverlapBytes < 0 {
		return errors.New("chunk byte configuration is invalid")
	}
	return nil
}

type Counter interface {
	Count(string) (int, error)
}

func Build(segments []postgres.TranscriptSegment, counter Counter, config Config) ([]Chunk, error) {
	if counter == nil {
		return nil, errors.New("chunk builder requires a length counter")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if len(segments) > MaxSourceSegments {
		return nil, fmt.Errorf("transcript exceeds %d source segments", MaxSourceSegments)
	}
	if len(segments) == 0 {
		return []Chunk{}, nil
	}
	units := make([]unit, 0, len(segments))
	for i, segment := range segments {
		if segment.Seq != i+1 || segment.Text == "" || !utf8.ValidString(segment.Text) || segment.EndMS <= segment.StartMS {
			return nil, fmt.Errorf("transcript segment %d is not a valid ordered source", i+1)
		}
		pieces, err := splitSegment(segment, 0, len(segment.Text), counter, config.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("split transcript segment %d: %w", segment.Seq, err)
		}
		units = append(units, pieces...)
	}

	chunks := make([]Chunk, 0)
	cursor := 0
	for cursor < len(units) {
		selected := make([]unit, 0, 8)
		if config.OverlapSentences == 1 && len(chunks) > 0 {
			previous := chunks[len(chunks)-1].members
			if len(previous) > 0 {
				candidate := previous[len(previous)-1]
				if candidate.completeSentence && candidate.byteCount <= config.MaxOverlapBytes {
					selected = append(selected, candidate)
				}
			}
		}
		freshCount := 0
		for cursor < len(units) {
			candidate := append(append([]unit(nil), selected...), units[cursor])
			text := join(candidate)
			byteCount, err := counter.Count(text)
			if err != nil {
				return nil, err
			}
			if byteCount > config.MaxBytes {
				if freshCount == 0 && len(selected) > 0 {
					selected = nil // The overlap is expendable; new source text is not.
					continue
				}
				if freshCount == 0 {
					return nil, errors.New("a split source unit still exceeds the chunk hard limit")
				}
				break
			}
			selected = candidate
			cursor++
			freshCount++
			if byteCount >= config.TargetBytes {
				break
			}
		}
		if freshCount == 0 || len(selected) == 0 {
			return nil, errors.New("chunk construction made no forward progress")
		}
		chunk, err := makeChunk(len(chunks)+1, selected, counter)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	if err := verifyCoverage(segments, chunks); err != nil {
		return nil, err
	}
	return chunks, nil
}

func splitSegment(segment postgres.TranscriptSegment, start, end int, counter Counter, maxBytes int) ([]unit, error) {
	text := segment.Text[start:end]
	byteCount, err := counter.Count(text)
	if err != nil {
		return nil, err
	}
	if byteCount <= maxBytes {
		return []unit{{segmentSeq: segment.Seq, startByte: start, endByte: end, text: text,
			byteCount: byteCount, completeSentence: start == 0 && end == len(segment.Text),
			startMS: segment.StartMS, endMS: segment.EndMS}}, nil
	}
	if end-start <= 1 {
		return nil, errors.New("one UTF-8 byte exceeds the chunk hard limit")
	}
	cut := chooseCut(text)
	if cut <= 0 || cut >= len(text) || !utf8.RuneStart(text[cut]) {
		return nil, errors.New("could not find a safe UTF-8 split boundary")
	}
	left, err := splitSegment(segment, start, start+cut, counter, maxBytes)
	if err != nil {
		return nil, err
	}
	right, err := splitSegment(segment, start+cut, end, counter, maxBytes)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

func chooseCut(text string) int {
	midpoint := len(text) / 2
	window := len(text) / 5
	if window < 1 {
		window = 1
	}
	best, bestDistance := 0, len(text)
	for index, current := range text {
		boundary := index + utf8.RuneLen(current)
		if boundary <= 0 || boundary >= len(text) || boundary < midpoint-window || boundary > midpoint+window {
			continue
		}
		if !isPreferredBoundary(current) {
			continue
		}
		distance := abs(boundary - midpoint)
		if distance < bestDistance {
			best, bestDistance = boundary, distance
		}
	}
	if best > 0 {
		return best
	}
	for midpoint < len(text) && !utf8.RuneStart(text[midpoint]) {
		midpoint++
	}
	if midpoint >= len(text) {
		midpoint = len(text) - 1
		for midpoint > 0 && !utf8.RuneStart(text[midpoint]) {
			midpoint--
		}
	}
	return midpoint
}

func isPreferredBoundary(r rune) bool {
	switch r {
	case '，', '。', '！', '？', '；', '：', ',', '.', '!', '?', ';', ':', '\n':
		return true
	default:
		return false
	}
}

func makeChunk(seq int, members []unit, counter Counter) (Chunk, error) {
	text := join(members)
	byteCount, err := counter.Count(text)
	if err != nil {
		return Chunk{}, err
	}
	chunk := Chunk{Seq: seq, Text: text, ByteCount: byteCount,
		StartMS: members[0].startMS, EndMS: members[0].endMS,
		Sources: make([]Source, 0, len(members)), members: append([]unit(nil), members...)}
	for _, member := range members {
		if member.startMS < chunk.StartMS {
			chunk.StartMS = member.startMS
		}
		if member.endMS > chunk.EndMS {
			chunk.EndMS = member.endMS
		}
		chunk.Sources = append(chunk.Sources, Source{TranscriptSegmentSeq: member.segmentSeq,
			StartByte: member.startByte, EndByte: member.endByte})
	}
	return chunk, nil
}

func join(units []unit) string {
	parts := make([]string, len(units))
	for i := range units {
		parts[i] = units[i].text
	}
	return strings.Join(parts, "\n")
}

func verifyCoverage(segments []postgres.TranscriptSegment, chunks []Chunk) error {
	coverage := make(map[int][][2]int, len(segments))
	for _, item := range chunks {
		for _, source := range item.Sources {
			coverage[source.TranscriptSegmentSeq] = append(coverage[source.TranscriptSegmentSeq], [2]int{source.StartByte, source.EndByte})
		}
	}
	for _, segment := range segments {
		intervals := coverage[segment.Seq]
		sort.Slice(intervals, func(i, j int) bool {
			if intervals[i][0] == intervals[j][0] {
				return intervals[i][1] < intervals[j][1]
			}
			return intervals[i][0] < intervals[j][0]
		})
		covered := 0
		for _, interval := range intervals {
			if interval[0] > covered {
				return fmt.Errorf("transcript segment %d has an uncovered text range", segment.Seq)
			}
			if interval[1] > covered {
				covered = interval[1]
			}
		}
		if covered != len(segment.Text) {
			return fmt.Errorf("transcript segment %d is not fully represented by chunks", segment.Seq)
		}
	}
	return nil
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
