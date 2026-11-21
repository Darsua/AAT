package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	errQuotaExhausted = errors.New("ai quota exhausted")
	errCircuitOpen    = errors.New("ai circuit breaker open")
	errTimeout        = errors.New("ai request timed out")
	errUpstream       = errors.New("ai upstream error")
)

// LLMGenerator generates short disaster situation summaries asynchronously.
type LLMGenerator interface {
	Generate(ctx context.Context, systemPrompt, userPrompt string) (string, error)
	CircuitState() string
}

// OpenAIChatClient calls an OpenAI-compatible /v1/chat/completions endpoint
// with strict timeout, sliding-window rate budget, and circuit breaker protection.
type OpenAIChatClient struct {
	apiURL           string
	model            string
	apiKey           string
	maxTokens        int
	timeout          time.Duration
	maxCallsPerMin   int
	failureThreshold int
	cooldown         time.Duration
	disableThinking  bool
	client           *http.Client

	mu          sync.Mutex
	callStamps  []time.Time
	failures    int
	openUntil   time.Time
	halfOpenTry bool
}

// NewOpenAIChatClient builds an isolated AI client that ignores ambient host proxy env vars by default.
func NewOpenAIChatClient(cfg Config) *OpenAIChatClient {
	transport := &http.Transport{
		Proxy: nil, // Isolate LAN LLM traffic from stale node HTTP_PROXY settings.
	}
	if cfg.AIUseEnvProxy {
		transport.Proxy = http.ProxyFromEnvironment
	}
	timeout := cfg.AITimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	cooldown := cfg.AICBCooldown
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	threshold := cfg.AICBFailureThreshold
	if threshold <= 0 {
		threshold = 3
	}
	return &OpenAIChatClient{
		apiURL:           cfg.AIAPIURL,
		model:            cfg.AIModel,
		apiKey:           cfg.AIAPIKey,
		maxTokens:        cfg.AIMaxTokens,
		timeout:          timeout,
		maxCallsPerMin:   cfg.AIMaxCallsPerMin,
		failureThreshold: threshold,
		cooldown:         cooldown,
		disableThinking:  !cfg.AIEnableThinking,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

// CircuitState reports the current breaker state ("closed", "open", or "half-open").
func (c *OpenAIChatClient) CircuitState() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if !c.openUntil.IsZero() {
		if now.Before(c.openUntil) {
			return "open"
		}
		return "half-open"
	}
	return "closed"
}

func (c *OpenAIChatClient) allowRequest(now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	isHalfOpenProbe := false
	if !c.openUntil.IsZero() {
		if now.Before(c.openUntil) {
			return errCircuitOpen
		}
		if c.halfOpenTry {
			return errCircuitOpen
		}
		isHalfOpenProbe = true
	}

	if c.maxCallsPerMin > 0 {
		cutoff := now.Add(-time.Minute)
		keep := c.callStamps[:0]
		for _, ts := range c.callStamps {
			if ts.After(cutoff) {
				keep = append(keep, ts)
			}
		}
		c.callStamps = keep
		if len(c.callStamps) >= c.maxCallsPerMin {
			return errQuotaExhausted
		}
		c.callStamps = append(c.callStamps, now)
	}
	if isHalfOpenProbe {
		c.halfOpenTry = true
	}
	return nil
}

func (c *OpenAIChatClient) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.openUntil = time.Time{}
	c.halfOpenTry = false
}

func (c *OpenAIChatClient) recordFailure(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.halfOpenTry = false
	c.failures++
	if c.failures >= c.failureThreshold || !c.openUntil.IsZero() {
		c.openUntil = now.Add(c.cooldown)
	}
}

type chatCompletionRequest struct {
	Model              string            `json:"model"`
	Messages           []chatMessage     `json:"messages"`
	MaxTokens          int               `json:"max_tokens,omitempty"`
	Temperature        float64           `json:"temperature"`
	ChatTemplateKwargs map[string]bool   `json:"chat_template_kwargs,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// Generate dispatches a single bounded chat-completion request.
func (c *OpenAIChatClient) Generate(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	if err := c.allowRequest(time.Now()); err != nil {
		return "", err
	}

	reqBody := chatCompletionRequest{
		Model: c.model,
		Messages: []chatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		MaxTokens:   c.maxTokens,
		Temperature: 0.2,
	}
	if c.disableThinking {
		reqBody.ChatTemplateKwargs = map[string]bool{"enable_thinking": false}
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("%w: marshal request: %v", errUpstream, err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.apiURL, bytes.NewReader(payload))
	if err != nil {
		c.recordFailure(time.Now())
		return "", fmt.Errorf("%w: build request: %v", errUpstream, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.recordFailure(time.Now())
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
			return "", errTimeout
		}
		return "", fmt.Errorf("%w: %v", errUpstream, err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		c.recordFailure(time.Now())
		return "", fmt.Errorf("%w: read body: %v", errUpstream, readErr)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		c.recordFailure(time.Now())
		return "", errQuotaExhausted
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.recordFailure(time.Now())
		return "", fmt.Errorf("%w: status %d", errUpstream, resp.StatusCode)
	}

	var parsed chatCompletionResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		c.recordFailure(time.Now())
		return "", fmt.Errorf("%w: decode response: %v", errUpstream, err)
	}
	if len(parsed.Choices) == 0 {
		c.recordFailure(time.Now())
		return "", fmt.Errorf("%w: empty choices", errUpstream)
	}

	c.recordSuccess()
	return parsed.Choices[0].Message.Content, nil
}

func classifyAIError(err error) string {
	switch {
	case errors.Is(err, errQuotaExhausted):
		return "quota_exhausted"
	case errors.Is(err, errCircuitOpen):
		return "circuit_open"
	case errors.Is(err, errTimeout), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "upstream_error"
	}
}

// validateSummaryOutput ensures non-empty, length-bounded, fact-grounded text.
func validateSummaryOutput(text string, events []HazardEvent) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || utf8.RuneCountInString(trimmed) > 1200 {
		return false
	}
	lower := strings.ToLower(trimmed)
	for _, ev := range events {
		for _, candidate := range []string{ev.HazardID, ev.AreaName, ev.Source, normalizeRegionKey(ev.AreaName)} {
			tok := strings.ToLower(strings.TrimSpace(candidate))
			if tok != "" && strings.Contains(lower, tok) {
				return true
			}
		}
	}
	return false
}
