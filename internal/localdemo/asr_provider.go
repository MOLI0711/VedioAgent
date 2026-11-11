package localdemo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"mirrorcore/internal/providers/funasr"
)

// ASRProvider emits a fixed transcript for the checked-in synthetic media clip.
// It exists to exercise durable submission and result-recovery code locally;
// it does not perform speech recognition.
type ASRProvider struct{}

const syntheticTranscript = `{"transcripts":[{"sentences":[{"begin_time":0,"end_time":3600,"text":"Kafka passes parse jobs from the API to workers."},{"begin_time":3600,"end_time":7100,"text":"A worker checks stored progress before doing the work again."}]}]}`

func (ASRProvider) Submit(ctx context.Context, audioURL string) (funasr.Task, error) {
	if err := ctx.Err(); err != nil {
		return funasr.Task{}, err
	}
	if !strings.HasPrefix(audioURL, "local-demo://"+LocalDemoBucket+"/") {
		return funasr.Task{}, errors.New("local demo ASR only accepts local-demo object URLs")
	}
	id := "local-" + uuid.NewString()
	return funasr.Task{ID: id, RequestID: "local-request-" + strings.TrimPrefix(id, "local-"), Status: "RUNNING"}, nil
}

func (ASRProvider) GetTask(ctx context.Context, taskID string) (funasr.Task, error) {
	if err := ctx.Err(); err != nil {
		return funasr.Task{}, err
	}
	if !strings.HasPrefix(taskID, "local-") {
		return funasr.Task{}, fmt.Errorf("unknown local demo ASR task %q", taskID)
	}
	return funasr.Task{ID: taskID, RequestID: "local-request-" + strings.TrimPrefix(taskID, "local-"),
		Status: "SUCCEEDED", SubtaskStatus: "SUCCEEDED", ResultURL: "local-demo://result/" + taskID}, nil
}

func (ASRProvider) DownloadResult(ctx context.Context, resultURL string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(resultURL, "local-demo://result/local-") {
		return nil, errors.New("invalid local demo ASR result URL")
	}
	return []byte(syntheticTranscript), nil
}
