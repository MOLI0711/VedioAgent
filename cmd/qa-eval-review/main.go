package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"mirrorcore/internal/agenteval"
)

const maxInputBytes = 64 << 20

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: qa-eval-review pack|apply [flags]")
	}
	switch args[0] {
	case "pack":
		return pack(args[1:])
	case "apply":
		return apply(args[1:])
	default:
		return errors.New("usage: qa-eval-review pack|apply [flags]")
	}
}

func pack(args []string) error {
	flags := flag.NewFlagSet("pack", flag.ContinueOnError)
	draftPath := flags.String("draft", "", "input annotation-draft JSONL")
	outputDir := flags.String("output-dir", "", "new private directory for review.jsonl and answer-key.json")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *draftPath == "" || *outputDir == "" || flags.NArg() != 0 {
		return errors.New("usage: qa-eval-review pack -draft annotation-draft.jsonl -output-dir new-private-directory")
	}
	draftBytes, err := readBounded(*draftPath)
	if err != nil {
		return err
	}
	samples, err := agenteval.LoadAnnotationDraftJSONL(bytes.NewReader(draftBytes))
	if err != nil {
		return err
	}
	packBytes, templateBytes, keyBytes, caseCount, err := agenteval.BuildBlindReviewArtifacts(draftBytes)
	if err != nil {
		return err
	}
	if err := writeNewReviewDirectory(*outputDir, packBytes, templateBytes, keyBytes); err != nil {
		return err
	}
	fmt.Printf("created blind review pack for %d cases in %s\n", caseCount, *outputDir)
	fmt.Println("share review.jsonl and review-template.jsonl with annotators; keep answer-key.json private")
	if samples[0].DatasetKind == agenteval.DatasetSynthetic {
		fmt.Println("notice: this is a synthetic fixture and cannot produce human quality metrics")
	}
	return nil
}

func apply(args []string) error {
	flags := flag.NewFlagSet("apply", flag.ContinueOnError)
	draftPath := flags.String("draft", "", "original annotation-draft JSONL")
	packPath := flags.String("pack", "", "unchanged review.jsonl from pack generation")
	keyPath := flags.String("key", "", "private answer-key.json from pack generation")
	submissionPath := flags.String("submission", "", "reviewer submission JSONL")
	outputPath := flags.String("output", "", "new output path for reviewed evaluation JSONL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *draftPath == "" || *packPath == "" || *keyPath == "" || *submissionPath == "" || *outputPath == "" || flags.NArg() != 0 {
		return errors.New("usage: qa-eval-review apply -draft draft.jsonl -pack review.jsonl -key answer-key.json -submission reviewer.jsonl -output reviewed.jsonl")
	}
	draftBytes, err := readBounded(*draftPath)
	if err != nil {
		return err
	}
	packBytes, err := readBounded(*packPath)
	if err != nil {
		return err
	}
	keyBytes, err := readBounded(*keyPath)
	if err != nil {
		return err
	}
	submissionBytes, err := readBounded(*submissionPath)
	if err != nil {
		return err
	}
	outputBytes, err := agenteval.ApplyBlindReviewArtifacts(draftBytes, packBytes, keyBytes, submissionBytes)
	if err != nil {
		return err
	}
	if err := writeNewFile(*outputPath, outputBytes, 0o600); err != nil {
		return fmt.Errorf("write reviewed dataset: %w", err)
	}
	fmt.Printf("applied blinded annotations for output dataset: %s\n", *outputPath)
	return nil
}

func readBounded(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxInputBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > maxInputBytes {
		return nil, fmt.Errorf("%s exceeds the %d MiB input limit", path, maxInputBytes>>20)
	}
	return data, nil
}

func writeNewReviewDirectory(path string, packBytes, templateBytes, keyBytes []byte) error {
	if err := os.Mkdir(path, 0o700); err != nil {
		return fmt.Errorf("create new private review directory: %w", err)
	}
	created := make([]string, 0, 3)
	cleanup := true
	defer func() {
		if cleanup {
			for _, name := range created {
				_ = os.Remove(name)
			}
			_ = os.Remove(path)
		}
	}()
	packPath := filepath.Join(path, "review.jsonl")
	if err := writeNewFile(packPath, packBytes, 0o600); err != nil {
		return fmt.Errorf("write anonymized reviewer pack: %w", err)
	}
	created = append(created, packPath)
	templatePath := filepath.Join(path, "review-template.jsonl")
	if err := writeNewFile(templatePath, templateBytes, 0o600); err != nil {
		return fmt.Errorf("write reviewer submission template: %w", err)
	}
	created = append(created, templatePath)
	keyPath := filepath.Join(path, "answer-key.json")
	if err := writeNewFile(keyPath, keyBytes, 0o600); err != nil {
		return fmt.Errorf("write private answer key: %w", err)
	}
	created = append(created, keyPath)
	cleanup = false
	return nil
}

func writeNewFile(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	createdInfo, statErr := file.Stat()
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if statErr != nil || writeErr != nil || closeErr != nil {
		currentInfo, currentErr := os.Stat(path)
		if statErr == nil && currentErr == nil && os.SameFile(createdInfo, currentInfo) {
			_ = os.Remove(path)
		}
		return errors.Join(statErr, writeErr, closeErr)
	}
	return nil
}
