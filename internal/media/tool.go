package media

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

const maxToolOutputBytes = 1 << 20

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		return 0, errors.New("media tool output exceeded the configured limit")
	}
	if len(p) > remaining {
		_, _ = b.Buffer.Write(p[:remaining])
		return remaining, errors.New("media tool output exceeded the configured limit")
	}
	return b.Buffer.Write(p)
}

func runTool(ctx context.Context, executable string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, executable, args...)
	var stdout, stderr boundedBuffer
	stdout.limit, stderr.limit = maxToolOutputBytes, 64<<10
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("media tool interrupted: %w", ctx.Err())
		}
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("media tool failed: %w: %s", err, stderr.String())
		}
		return nil, fmt.Errorf("media tool failed: %w", err)
	}
	return stdout.Bytes(), nil
}

func ToolVersion(ctx context.Context, executable string) (string, error) {
	output, err := runTool(ctx, executable, "-version")
	if err != nil {
		return "", fmt.Errorf("read media tool version: %w", err)
	}
	firstLine := strings.TrimSpace(strings.SplitN(string(output), "\n", 2)[0])
	if firstLine == "" || len(firstLine) > 200 {
		return "", errors.New("media tool returned an invalid version string")
	}
	return firstLine, nil
}
