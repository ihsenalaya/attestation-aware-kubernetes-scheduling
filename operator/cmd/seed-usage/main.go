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

// Command seed-usage produces REAL telemetry for the operator demo. It issues a
// handful of real LLM calls per demo application across two real providers
// (OpenAI gpt-4o in the US, Mistral-Large via Azure AI Foundry in the EU),
// measures the actual prompt/completion tokens and latency returned by each API,
// then projects those measured per-call profiles to a realistic monthly request
// volume. The result is a JSON array of usage samples (the shape the configmap
// telemetry collector reads) printed to stdout or written with -out.
//
// Costs are NOT computed here — the operator's cost engine multiplies these real
// token volumes by the real per-model prices on the AIProviders. So the demo
// reflects genuine token economics: real measured usage x real list prices.
//
// Keys are read from files/flags and never printed. Self-contained (no external
// SDK): a minimal OpenAI-compatible chat client supporting OpenAI Bearer auth and
// Azure AI Foundry api-key auth.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// record matches the JSON the configmap collector reads (internal/collectors/configmap).
type record struct {
	Namespace     string  `json:"namespace"`
	Application   string  `json:"application"`
	Team          string  `json:"team"`
	Provider      string  `json:"provider"`
	Model         string  `json:"model"`
	Requests      int64   `json:"requests"`
	InputTokens   int64   `json:"inputTokens"`
	OutputTokens  int64   `json:"outputTokens"`
	LatencyMillis float64 `json:"latencyMillis"`
	Errors        int64   `json:"errors"`
}

// client is a minimal OpenAI-compatible chat client.
type client struct {
	base   string // .../v1 (OpenAI) or .../models (Foundry); /chat/completions is appended
	key    string
	apiKey bool   // true => "api-key" header (Foundry), else Authorization: Bearer
	query  string // e.g. api-version=...
	http   *http.Client
}

type chatResp struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// chat issues one chat completion and returns (inputTokens, outputTokens, latencyMS).
func (c *client) chat(ctx context.Context, model, prompt string, maxTokens int) (int, int, int64, error) {
	body, _ := json.Marshal(map[string]any{
		"model":       model,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"max_tokens":  maxTokens,
		"temperature": 0.3,
	})
	url := strings.TrimRight(c.base, "/") + "/chat/completions"
	if c.query != "" {
		url += "?" + c.query
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
	}
	if c.apiKey {
		req.Header.Set("api-key", c.key)
	} else {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	lat := time.Since(start).Milliseconds()
	if resp.StatusCode >= 300 {
		return 0, 0, lat, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(raw), 160))
	}
	var cr chatResp
	if err := json.Unmarshal(raw, &cr); err != nil {
		return 0, 0, lat, fmt.Errorf("decode: %w", err)
	}
	if cr.Error != nil {
		return 0, 0, lat, fmt.Errorf("api error: %s", cr.Error.Message)
	}
	return cr.Usage.PromptTokens, cr.Usage.CompletionTokens, lat, nil
}

// app is one demo workload mapped to a real provider/model. requestsPerDay is the
// real-world observable used ONLY when -project-days is set, to forecast a longer
// horizon from the measured per-call profile (forecast = measured/call x perDay x days).
type app struct {
	namespace, application, team string
	provider, model              string // catalog labels (must match config/samples)
	callModel                    string // the actual model/deployment name to call
	useFoundry                   bool
	requestsPerDay               int64
	prompts                      []string
}

func main() {
	openaiKeyPath := flag.String("openai-key", "docs/openaikey.txt", "path to OpenAI API key file")
	foundryBase := flag.String("foundry-base", "", "Azure AI Foundry models endpoint base, e.g. https://<acct>.services.ai.azure.com/models")
	foundryKey := flag.String("foundry-key", "", "Azure AI Foundry API key (value)")
	foundryDeploy := flag.String("foundry-deployment", "mistral-large-latest", "Foundry Mistral deployment name to call")
	foundryAPIVer := flag.String("foundry-api-version", "2024-05-01-preview", "Foundry api-version")
	calls := flag.Int("calls", 3, "real calls per app")
	maxTokens := flag.Int("max-tokens", 256, "max output tokens per call")
	projectDays := flag.Int("project-days", 0, "if >0, forecast usage = measured-per-call x requestsPerDay x project-days (e.g. 30 for a month); default 0 = report the REAL observed totals only")
	out := flag.String("out", "", "output file (default: stdout)")
	flag.Parse()

	openaiKey, err := loadKey(*openaiKeyPath)
	if err != nil {
		fatal("OpenAI key: %v", err)
	}
	openai := &client{base: "https://api.openai.com/v1", key: openaiKey, http: &http.Client{Timeout: 120 * time.Second}}

	var foundry *client
	if *foundryBase != "" && *foundryKey != "" {
		q := ""
		if *foundryAPIVer != "" {
			q = "api-version=" + *foundryAPIVer
		}
		foundry = &client{base: *foundryBase, key: *foundryKey, apiKey: true, query: q, http: &http.Client{Timeout: 120 * time.Second}}
	}

	apps := []app{
		{
			namespace: "rh", application: "chatbot-rh", team: "rh",
			provider: "mistral-eu", model: "mistral-large", callModel: *foundryDeploy, useFoundry: true,
			requestsPerDay: 1000,
			prompts: []string{
				"Explique en deux phrases la politique de conges payes en France.",
				"Redige un court message d'accueil pour un nouvel employe.",
				"Quels documents fournir pour une note de frais ?",
			},
		},
		{
			namespace: "finance", application: "risk-assistant", team: "finance",
			provider: "openai-us", model: "gpt-4o", callModel: "gpt-4o", useFoundry: false,
			requestsPerDay: 260,
			prompts: []string{
				"Summarize the key counterparty risks when onboarding a new supplier.",
				"List three indicators of liquidity risk for a mid-cap company.",
				"Explain value-at-risk to a non-technical executive in two sentences.",
			},
		},
	}

	ctx := context.Background()
	var recs []record
	for _, a := range apps {
		c := openai
		if a.useFoundry {
			if foundry == nil {
				warn("skipping %s/%s: no Foundry endpoint/key provided", a.namespace, a.application)
				continue
			}
			c = foundry
		}
		var sumIn, sumOut, n, errs int64
		var sumLat float64
		for i := 0; i < *calls; i++ {
			in, outTok, lat, err := c.chat(ctx, a.callModel, a.prompts[i%len(a.prompts)], *maxTokens)
			if err != nil {
				errs++
				warn("%s/%s call %d failed: %v", a.namespace, a.application, i+1, err)
				continue
			}
			sumIn += int64(in)
			sumOut += int64(outTok)
			sumLat += float64(lat)
			n++
			fmt.Fprintf(os.Stderr, "  %s/%s via %s: in=%d out=%d %dms\n", a.namespace, a.application, a.callModel, in, outTok, lat)
			time.Sleep(300 * time.Millisecond)
		}
		if n == 0 {
			warn("no successful calls for %s/%s; skipping", a.namespace, a.application)
			continue
		}
		avgLat := sumLat / float64(n)
		rec := record{
			Namespace: a.namespace, Application: a.application, Team: a.team,
			Provider: a.provider, Model: a.model,
			LatencyMillis: avgLat,
		}
		if *projectDays > 0 {
			// Forecast: measured per-call profile x requestsPerDay x project-days.
			total := a.requestsPerDay * int64(*projectDays)
			avgIn := float64(sumIn) / float64(n)
			avgOut := float64(sumOut) / float64(n)
			errRate := float64(errs) / float64(*calls)
			rec.Requests = total
			rec.InputTokens = int64(avgIn * float64(total))
			rec.OutputTokens = int64(avgOut * float64(total))
			rec.Errors = int64(errRate * float64(total))
			fmt.Fprintf(os.Stderr, "=> %s/%s: avg in=%.0f out=%.0f tok/call; FORECAST %d req/day x %d days = %d req\n",
				a.namespace, a.application, avgIn, avgOut, a.requestsPerDay, *projectDays, total)
		} else {
			// Real observed totals: exactly the tokens the API returned for these calls.
			rec.Requests = n
			rec.InputTokens = sumIn
			rec.OutputTokens = sumOut
			rec.Errors = errs
			fmt.Fprintf(os.Stderr, "=> %s/%s: REAL observed %d call(s): in=%d out=%d tokens, %.0fms avg\n",
				a.namespace, a.application, n, sumIn, sumOut, avgLat)
		}
		recs = append(recs, rec)
	}

	if len(recs) == 0 {
		fatal("no usage produced (all calls failed?)")
	}
	data, _ := json.MarshalIndent(recs, "", "  ")
	if *out == "" {
		fmt.Println(string(data))
		return
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fatal("write %s: %v", *out, err)
	}
	fmt.Fprintf(os.Stderr, "wrote %d usage sample(s) to %s\n", len(recs), *out)
}

var skRe = regexp.MustCompile(`sk-[A-Za-z0-9_-]+`)

// loadKey reads an API key from a file ("sk-..." or "OPENAI_API_KEY=sk-...") and
// falls back to the OPENAI_API_KEY env var. The key is never logged.
func loadKey(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		if m := skRe.FindString(string(b)); m != "" {
			return m, nil
		}
		if s := strings.TrimSpace(string(b)); s != "" {
			return s, nil
		}
	}
	if env := strings.TrimSpace(os.Getenv("OPENAI_API_KEY")); env != "" {
		return env, nil
	}
	return "", fmt.Errorf("no API key found at %s or in OPENAI_API_KEY", path)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func warn(f string, a ...any)  { fmt.Fprintf(os.Stderr, "WARN: "+f+"\n", a...) }
func fatal(f string, a ...any) { fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", a...); os.Exit(1) }
