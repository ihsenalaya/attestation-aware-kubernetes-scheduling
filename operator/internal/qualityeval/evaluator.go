/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package qualityeval runs golden prompts against the production AI gateway and
// serializes response evidence for the pure qualityengine.
package qualityeval

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/imperium/ai-sovereign-finops-operator/internal/qualityengine"
)

const (
	defaultMaxTokens = 96
	maxActualBytes   = 700
)

// Options configures one gateway-backed evaluation run.
type Options struct {
	Endpoint       string
	PromptsDir     string
	PromptsFile    string
	Namespace      string
	Application    string
	SourceModel    string
	CandidateModel string
	MaxTokens      int
	Timeout        time.Duration
	HTTPClient     *http.Client
}

// GoldenPrompt mirrors the AIQualityGate golden dataset format.
type GoldenPrompt struct {
	ID       string `json:"id"`
	Prompt   string `json:"prompt"`
	Expected struct {
		RequiredKeywords []string          `json:"requiredKeywords,omitempty"`
		MaxTokens        int32             `json:"maxTokens,omitempty"`
		MustBeJSON       bool              `json:"mustBeJSON,omitempty"`
		Reference        string            `json:"reference,omitempty"`
		Fields           map[string]string `json:"fields,omitempty"`
	} `json:"expected,omitempty"`
}

// EvidenceSample is the serialized response evidence consumed by AIQualityGate.
type EvidenceSample struct {
	ID                      string            `json:"id"`
	Model                   string            `json:"model,omitempty"`
	Reference               string            `json:"reference,omitempty"`
	Actual                  string            `json:"actual,omitempty"`
	ExpectedFields          map[string]string `json:"expectedFields,omitempty"`
	ActualFields            map[string]string `json:"actualFields,omitempty"`
	SemanticScore           *float64          `json:"semanticScore,omitempty"`
	SchemaValid             *bool             `json:"schemaValid,omitempty"`
	UnexpectedRefusal       *bool             `json:"unexpectedRefusal,omitempty"`
	SensitiveDataLeak       *bool             `json:"sensitiveDataLeak,omitempty"`
	RequiredKeywordsPresent *bool             `json:"requiredKeywordsPresent,omitempty"`
}

// EvidenceDocument is written to the AIQualityGate evidence ConfigMap.
type EvidenceDocument struct {
	Samples []EvidenceSample `json:"samples"`
}

// Run evaluates source and candidate models and returns JSON evidence.
func Run(ctx context.Context, opts Options) ([]byte, error) {
	opts.Endpoint = strings.TrimSpace(opts.Endpoint)
	if opts.Endpoint == "" {
		return nil, fmt.Errorf("evaluation endpoint is empty")
	}
	if strings.TrimSpace(opts.SourceModel) == "" || strings.TrimSpace(opts.CandidateModel) == "" {
		return nil, fmt.Errorf("source and candidate model are required")
	}
	if opts.MaxTokens <= 0 {
		opts.MaxTokens = defaultMaxTokens
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 60 * time.Second
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: opts.Timeout}
	}

	prompts, err := LoadPrompts(opts.PromptsDir, opts.PromptsFile)
	if err != nil {
		return nil, err
	}
	if len(prompts) == 0 {
		return nil, fmt.Errorf("golden dataset contains no prompts")
	}

	models := []string{opts.SourceModel}
	if opts.CandidateModel != opts.SourceModel {
		models = append(models, opts.CandidateModel)
	}

	doc := EvidenceDocument{Samples: make([]EvidenceSample, 0, len(prompts)*len(models))}
	for _, prompt := range prompts {
		if strings.TrimSpace(prompt.ID) == "" || strings.TrimSpace(prompt.Prompt) == "" {
			return nil, fmt.Errorf("golden prompt %q is missing id or prompt text", prompt.ID)
		}
		for _, model := range models {
			actual, err := callGateway(ctx, client, opts, prompt, model)
			if err != nil {
				return nil, err
			}
			doc.Samples = append(doc.Samples, evidenceFor(prompt, model, actual))
		}
	}

	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("serialize quality evidence: %w", err)
	}
	return raw, nil
}

// LoadPrompts reads a golden dataset from a mounted ConfigMap directory or file.
func LoadPrompts(dir, file string) ([]GoldenPrompt, error) {
	var raw []byte
	var err error
	if strings.TrimSpace(file) != "" {
		raw, err = os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read prompts file %s: %w", file, err)
		}
	} else {
		if strings.TrimSpace(dir) == "" {
			return nil, fmt.Errorf("prompts-dir or prompts-file is required")
		}
		for _, name := range []string{"prompts.yaml", "prompts.yml", "prompts.json"} {
			path := filepath.Join(dir, name)
			raw, err = os.ReadFile(path)
			if err == nil {
				break
			}
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("read prompts file %s: %w", path, err)
			}
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("no prompts.yaml, prompts.yml or prompts.json found in %s", dir)
		}
	}

	var prompts []GoldenPrompt
	if err := yaml.Unmarshal(raw, &prompts); err == nil && len(prompts) > 0 {
		return prompts, nil
	}
	var wrapped struct {
		Prompts []GoldenPrompt `json:"prompts"`
	}
	if err := yaml.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("parse golden dataset: %w", err)
	}
	return wrapped.Prompts, nil
}

func callGateway(ctx context.Context, client *http.Client, opts Options, prompt GoldenPrompt, model string) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		content, err := callGatewayOnce(ctx, client, opts, prompt, model)
		if err == nil {
			return content, nil
		}
		lastErr = err
		if attempt == 5 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt*2) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", lastErr
}

func callGatewayOnce(ctx context.Context, client *http.Client, opts Options, prompt GoldenPrompt, model string) (string, error) {
	maxTokens := opts.MaxTokens
	if prompt.Expected.MaxTokens > 0 {
		maxTokens = int(prompt.Expected.MaxTokens)
	}
	payload := map[string]any{
		"model":       model,
		"temperature": 0,
		"max_tokens":  maxTokens,
		"messages": []map[string]string{{
			"role":    "user",
			"content": prompt.Prompt,
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal request for prompt %q model %q: %w", prompt.ID, model, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request for prompt %q model %q: %w", prompt.ID, model, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-ai-eg-model", model)
	if opts.Namespace != "" {
		req.Header.Set("x-greenops-namespace", opts.Namespace)
	}
	if opts.Application != "" {
		req.Header.Set("x-greenops-app", opts.Application)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gateway call failed for prompt %q model %q: %w", prompt.ID, model, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("gateway call for prompt %q model %q returned HTTP %d: %s", prompt.ID, model, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	content, err := extractChatContent(raw)
	if err != nil {
		return "", fmt.Errorf("parse gateway response for prompt %q model %q: %w", prompt.ID, model, err)
	}
	return truncateForEvidence(content), nil
}

func extractChatContent(raw []byte) (string, error) {
	var response struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			Text string `json:"text"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", err
	}
	for _, choice := range response.Choices {
		if strings.TrimSpace(choice.Message.Content) != "" {
			return choice.Message.Content, nil
		}
		if strings.TrimSpace(choice.Text) != "" {
			return choice.Text, nil
		}
	}
	return "", fmt.Errorf("response contains no choice content")
}

func evidenceFor(prompt GoldenPrompt, model, actual string) EvidenceSample {
	reference := prompt.Expected.Reference
	if reference == "" && len(prompt.Expected.RequiredKeywords) > 0 {
		reference = strings.Join(prompt.Expected.RequiredKeywords, " ")
	}
	schemaValid := true
	var actualFields map[string]string
	if prompt.Expected.MustBeJSON || len(prompt.Expected.Fields) > 0 {
		schemaValid = json.Valid([]byte(actual))
		if schemaValid {
			actualFields = parseStringFields(actual)
		}
	}
	var semanticScore *float64
	if strings.TrimSpace(reference) != "" {
		semantic := qualityengine.SemanticSimilarityScore(reference, actual)
		if len(prompt.Expected.RequiredKeywords) > 0 {
			semantic = 0.75*semantic + 0.25*keywordCoverageScore(actual, prompt.Expected.RequiredKeywords)
		}
		semanticScore = &semantic
	}
	requiredKeywordsPresent := containsAll(actual, prompt.Expected.RequiredKeywords)
	unexpectedRefusal := looksLikeRefusal(actual)
	sensitiveDataLeak := false
	return EvidenceSample{
		ID:                      prompt.ID,
		Model:                   model,
		Reference:               reference,
		Actual:                  actual,
		ExpectedFields:          prompt.Expected.Fields,
		ActualFields:            actualFields,
		SemanticScore:           semanticScore,
		SchemaValid:             &schemaValid,
		UnexpectedRefusal:       &unexpectedRefusal,
		SensitiveDataLeak:       &sensitiveDataLeak,
		RequiredKeywordsPresent: &requiredKeywordsPresent,
	}
}

func parseStringFields(raw string) map[string]string {
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil
	}
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		switch typed := v.(type) {
		case string:
			out[k] = typed
		case float64, bool:
			out[k] = fmt.Sprint(typed)
		}
	}
	return out
}

func containsAll(text string, keywords []string) bool {
	lower := foldLatin(strings.ToLower(text))
	for _, kw := range keywords {
		kw = foldLatin(strings.ToLower(strings.TrimSpace(kw)))
		if kw == "" {
			continue
		}
		if !strings.Contains(lower, kw) {
			return false
		}
	}
	return true
}

func keywordCoverageScore(text string, keywords []string) float64 {
	var total, present int
	lower := foldLatin(strings.ToLower(text))
	for _, kw := range keywords {
		kw = foldLatin(strings.ToLower(strings.TrimSpace(kw)))
		if kw == "" {
			continue
		}
		total++
		if strings.Contains(lower, kw) {
			present++
		}
	}
	if total == 0 {
		return 100
	}
	return 100 * float64(present) / float64(total)
}

func foldLatin(s string) string {
	replacer := strings.NewReplacer(
		"à", "a", "â", "a", "ä", "a",
		"ç", "c",
		"é", "e", "è", "e", "ê", "e", "ë", "e",
		"î", "i", "ï", "i",
		"ô", "o", "ö", "o",
		"ù", "u", "û", "u", "ü", "u",
	)
	return replacer.Replace(s)
}

func looksLikeRefusal(text string) bool {
	lower := strings.ToLower(text)
	needles := []string{
		"i can't",
		"i cannot",
		"je ne peux pas",
		"je ne peux",
		"cannot comply",
		"can't assist",
	}
	for _, needle := range needles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func truncateForEvidence(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= maxActualBytes {
		return text
	}
	return text[:maxActualBytes]
}
