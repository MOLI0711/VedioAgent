package funasr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxAPIResponseBytes = 2 << 20
	MaxResultBytes      = 64 << 20
)

type Client struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

type Task struct {
	ID            string
	RequestID     string
	Status        string
	SubtaskStatus string
	ResultURL     string
	Code          string
	Message       string
}

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RetryAfter time.Duration
	Ambiguous  bool
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Fun-ASR HTTP %d (%s)", e.StatusCode, e.Code)
}

func New(workspaceID, apiKey string) (*Client, error) {
	if workspaceID == "" || apiKey == "" {
		return nil, errors.New("Fun-ASR workspace and API key are required")
	}
	transport := &http.Transport{
		Proxy:        http.ProxyFromEnvironment,
		DialContext:  (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: 16, MaxIdleConnsPerHost: 8,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 25 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &Client{
		endpoint: "https://" + workspaceID + ".cn-beijing.maas.aliyuncs.com/api/v1",
		apiKey:   apiKey,
		http: &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func (c *Client) Submit(ctx context.Context, audioURL string) (Task, error) {
	payload := map[string]any{
		"model":      "fun-asr",
		"input":      map[string]any{"file_urls": []string{audioURL}},
		"parameters": map[string]any{"channel_id": []int{0}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Task{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/services/audio/asr/transcription", bytes.NewReader(body))
	if err != nil {
		return Task{}, err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-DashScope-Async", "enable")
	response, err := c.http.Do(request)
	if err != nil {
		return Task{}, &APIError{Code: "SUBMIT_TRANSPORT_UNKNOWN", Message: "submission outcome is unknown", Ambiguous: true}
	}
	defer response.Body.Close()
	responseBody, err := readBounded(response.Body, maxAPIResponseBytes)
	if err != nil {
		return Task{}, &APIError{Code: "SUBMIT_RESPONSE_UNKNOWN", Message: "submission response could not be read", Ambiguous: true}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Task{}, parseAPIError(response, responseBody)
	}
	var result struct {
		RequestID string `json:"request_id"`
		Output    struct {
			TaskID     string `json:"task_id"`
			TaskStatus string `json:"task_status"`
		} `json:"output"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil || result.Output.TaskID == "" {
		return Task{}, &APIError{Code: "SUBMIT_RESPONSE_UNKNOWN", Message: "successful HTTP response did not contain a task_id", Ambiguous: true}
	}
	return Task{ID: result.Output.TaskID, RequestID: result.RequestID, Status: result.Output.TaskStatus}, nil
}

func (c *Client) GetTask(ctx context.Context, taskID string) (Task, error) {
	if taskID == "" || strings.ContainsAny(taskID, "/?#") {
		return Task{}, errors.New("invalid Fun-ASR task ID")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.endpoint+"/tasks/"+url.PathEscape(taskID), nil)
	if err != nil {
		return Task{}, err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	response, err := c.http.Do(request)
	if err != nil {
		return Task{}, fmt.Errorf("query Fun-ASR task: %w", err)
	}
	defer response.Body.Close()
	body, err := readBounded(response.Body, maxAPIResponseBytes)
	if err != nil {
		return Task{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Task{}, parseAPIError(response, body)
	}
	var result struct {
		RequestID string `json:"request_id"`
		Output    struct {
			TaskID     string `json:"task_id"`
			TaskStatus string `json:"task_status"`
			Results    []struct {
				SubtaskStatus    string `json:"subtask_status"`
				TranscriptionURL string `json:"transcription_url"`
				Code             string `json:"code"`
				Message          string `json:"message"`
			} `json:"results"`
		} `json:"output"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return Task{}, fmt.Errorf("decode Fun-ASR task response: %w", err)
	}
	if result.Output.TaskID != taskID {
		return Task{}, errors.New("Fun-ASR query did not confirm the requested task ID")
	}
	task := Task{ID: result.Output.TaskID, RequestID: result.RequestID, Status: result.Output.TaskStatus}
	if len(result.Output.Results) > 1 {
		return Task{}, errors.New("Fun-ASR returned more than one subtask for a single audio input")
	}
	if len(result.Output.Results) == 1 {
		item := result.Output.Results[0]
		task.SubtaskStatus, task.ResultURL, task.Code, task.Message = item.SubtaskStatus, item.TranscriptionURL, item.Code, item.Message
	}
	return task, nil
}

func (c *Client) DownloadResult(ctx context.Context, resultURL string) ([]byte, error) {
	parsed, err := url.Parse(resultURL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Host == "" || (parsed.Port() != "" && parsed.Port() != "443") ||
		!strings.HasSuffix(strings.ToLower(parsed.Hostname()), ".aliyuncs.com") {
		return nil, errors.New("Fun-ASR result URL is not an allowed HTTPS Aliyun host")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("download Fun-ASR result: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := readBounded(response.Body, maxAPIResponseBytes)
		return nil, parseAPIError(response, body)
	}
	body, err := readBounded(response.Body, MaxResultBytes)
	if err != nil {
		return nil, err
	}
	if err := ValidateResult(body); err != nil {
		return nil, err
	}
	return body, nil
}

func ValidateResult(body []byte) error {
	var parsedResult struct {
		Transcripts []struct {
			Sentences []struct {
				BeginTime int64  `json:"begin_time"`
				EndTime   int64  `json:"end_time"`
				Text      string `json:"text"`
			} `json:"sentences"`
		} `json:"transcripts"`
	}
	if err := json.Unmarshal(body, &parsedResult); err != nil {
		return fmt.Errorf("Fun-ASR result is not valid transcript JSON: %w", err)
	}
	if parsedResult.Transcripts == nil {
		return errors.New("Fun-ASR result lacks transcripts")
	}
	for _, transcript := range parsedResult.Transcripts {
		for _, sentence := range transcript.Sentences {
			if sentence.BeginTime < 0 || sentence.EndTime < sentence.BeginTime {
				return errors.New("Fun-ASR result contains an invalid sentence interval")
			}
		}
	}
	return nil
}

func readBounded(reader io.Reader, max int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("response exceeds %s bytes", strconv.FormatInt(max, 10))
	}
	return body, nil
}

func parseAPIError(response *http.Response, body []byte) *APIError {
	var detail struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(body, &detail)
	apiErr := &APIError{StatusCode: response.StatusCode, Code: detail.Code, Message: detail.Message}
	if response.StatusCode == http.StatusTooManyRequests {
		apiErr.RetryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
	} else if (response.StatusCode >= 300 && response.StatusCode < 400) || response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout {
		apiErr.Ambiguous = true
	}
	return apiErr
}

func parseRetryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return delay
		}
	}
	return time.Second
}
