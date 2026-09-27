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

// Package qualityengine computes auditable model quality scores from golden
// evidence and real telemetry. It is deliberately pure: callers inject any
// semantic scorer or LLM judge output through data structures, not network calls.
package qualityengine

import (
	"encoding/json"
	"math"
	"strings"
	"unicode"
)

const (
	VerdictCandidateSafe    = "candidate-safe"
	VerdictCandidateRisk    = "candidate-risk"
	VerdictInsufficientData = "insufficient-data"
)

// Weights configures the composite score. Values are normalized before use.
type Weights struct {
	Correctness float64
	Reliability float64
	Latency     float64
	Semantic    float64
	Judged      float64
}

// DefaultWeights returns the product defaults from the AI Quality Score spec.
func DefaultWeights() Weights {
	return Weights{Correctness: 0.40, Reliability: 0.20, Latency: 0.15, Semantic: 0.15, Judged: 0.10}
}

// NormalizeWeights clamps negative weights to zero and scales the sum to one.
func NormalizeWeights(w Weights) Weights {
	w = Weights{
		Correctness: math.Max(0, w.Correctness),
		Reliability: math.Max(0, w.Reliability),
		Latency:     math.Max(0, w.Latency),
		Semantic:    math.Max(0, w.Semantic),
		Judged:      math.Max(0, w.Judged),
	}
	sum := w.Correctness + w.Reliability + w.Latency + w.Semantic + w.Judged
	if sum <= 0 {
		return DefaultWeights()
	}
	return Weights{
		Correctness: w.Correctness / sum,
		Reliability: w.Reliability / sum,
		Latency:     w.Latency / sum,
		Semantic:    w.Semantic / sum,
		Judged:      w.Judged / sum,
	}
}

// Telemetry is the real operational signal for one application/model pair.
type Telemetry struct {
	Requests        int64
	Errors          int64
	Timeouts        int64
	InvalidJSON     int64
	LatencyMillis   float64
	LatencyObserved bool
}

// EvidenceSample is one golden prompt result for one model.
type EvidenceSample struct {
	ID                      string
	Expected                string
	Actual                  string
	MustBeJSON              bool
	ExpectedFields          map[string]string
	ActualFields            map[string]string
	RequiredKeywordsPresent *bool
	SemanticScore           *float64
	JudgedScore             *float64
}

// DimensionScores contains normalized 0..100 scores.
type DimensionScores struct {
	Correctness float64
	Reliability float64
	Latency     float64
	Semantic    float64
	Judged      float64
}

// ModelResult is the computed score for one model.
type ModelResult struct {
	Overall        float64
	Breakdown      DimensionScores
	WeightsUsed    Weights
	Samples        int
	MissingSignals []string
	Insufficient   bool
}

// Comparison is the source/candidate decision.
type Comparison struct {
	Source       ModelResult
	Candidate    ModelResult
	Verdict      string
	Reason       string
	MinSamples   int
	Tolerance    float64
	WeightsUsed  Weights
	MissingInput []string
}

// EvaluateInput holds source and candidate evidence for one gate.
type EvaluateInput struct {
	SourceSamples      []EvidenceSample
	CandidateSamples   []EvidenceSample
	SourceTelemetry    Telemetry
	CandidateTelemetry Telemetry
	Weights            Weights
	MinSamples         int
	TolerancePoints    float64
	LatencyThresholdMs float64
	JudgeEnabled       bool
}

// Evaluate compares candidate quality against source quality.
func Evaluate(in EvaluateInput) Comparison {
	if in.MinSamples <= 0 {
		in.MinSamples = 1
	}
	if in.TolerancePoints == 0 {
		in.TolerancePoints = 3
	}
	weights := in.Weights
	if weights == (Weights{}) {
		weights = DefaultWeights()
	}
	if !in.JudgeEnabled {
		weights.Judged = 0
	}
	weights = NormalizeWeights(weights)

	out := Comparison{MinSamples: in.MinSamples, Tolerance: in.TolerancePoints, WeightsUsed: weights}
	if len(in.SourceSamples) < in.MinSamples {
		out.MissingInput = append(out.MissingInput, "source golden evidence below minSamples")
	}
	if len(in.CandidateSamples) < in.MinSamples {
		out.MissingInput = append(out.MissingInput, "candidate golden evidence below minSamples")
	}

	out.Source = EvaluateModel(in.SourceSamples, in.SourceTelemetry, weights, in.LatencyThresholdMs)
	out.Candidate = EvaluateModel(in.CandidateSamples, in.CandidateTelemetry, weights, in.LatencyThresholdMs)
	if out.Source.Insufficient {
		out.MissingInput = append(out.MissingInput, prefixSignals("source", out.Source.MissingSignals)...)
	}
	if out.Candidate.Insufficient {
		out.MissingInput = append(out.MissingInput, prefixSignals("candidate", out.Candidate.MissingSignals)...)
	}
	if len(out.MissingInput) > 0 {
		out.Verdict = VerdictInsufficientData
		out.Reason = strings.Join(out.MissingInput, "; ")
		return out
	}

	if out.Candidate.Overall >= out.Source.Overall-in.TolerancePoints {
		out.Verdict = VerdictCandidateSafe
		out.Reason = "candidate score is within tolerance of source"
	} else {
		out.Verdict = VerdictCandidateRisk
		out.Reason = "candidate score is below source tolerance"
	}
	return out
}

// EvaluateModel computes one model score. Missing weighted dimensions make the
// result insufficient; callers can set a dimension weight to zero to opt out.
func EvaluateModel(samples []EvidenceSample, telemetry Telemetry, weights Weights, latencyThresholdMs float64) ModelResult {
	weights = NormalizeWeights(weights)
	out := ModelResult{WeightsUsed: weights, Samples: len(samples)}
	score, ok := CorrectnessScore(samples)
	if ok {
		out.Breakdown.Correctness = score
	} else if weights.Correctness > 0 {
		out.MissingSignals = append(out.MissingSignals, "correctness evidence missing")
	}
	score, ok = ReliabilityScore(telemetry)
	if ok {
		out.Breakdown.Reliability = score
	} else if weights.Reliability > 0 {
		out.MissingSignals = append(out.MissingSignals, "real reliability telemetry missing")
	}
	score, ok = LatencyScore(telemetry, latencyThresholdMs)
	if ok {
		out.Breakdown.Latency = score
	} else if weights.Latency > 0 {
		out.MissingSignals = append(out.MissingSignals, "real latency telemetry missing")
	}
	score, ok = SemanticScore(samples)
	if ok {
		out.Breakdown.Semantic = score
	} else if weights.Semantic > 0 {
		out.MissingSignals = append(out.MissingSignals, "semantic score missing")
	}
	score, ok = JudgedScore(samples)
	if ok {
		out.Breakdown.Judged = score
	} else if weights.Judged > 0 {
		out.MissingSignals = append(out.MissingSignals, "judge score missing")
	}
	if len(out.MissingSignals) > 0 {
		out.Insufficient = true
		return out
	}
	out.Overall = clamp100(
		weights.Correctness*out.Breakdown.Correctness +
			weights.Reliability*out.Breakdown.Reliability +
			weights.Latency*out.Breakdown.Latency +
			weights.Semantic*out.Breakdown.Semantic +
			weights.Judged*out.Breakdown.Judged,
	)
	return out
}

// CorrectnessScore averages deterministic response checks over the evidence set.
func CorrectnessScore(samples []EvidenceSample) (float64, bool) {
	var total float64
	var count int
	for _, s := range samples {
		var sampleTotal float64
		var sampleCount int
		if strings.TrimSpace(s.Expected) != "" || strings.TrimSpace(s.Actual) != "" {
			sampleTotal += ReferenceCorrectnessScore(s.Expected, s.Actual)
			sampleCount++
		}
		if s.RequiredKeywordsPresent != nil {
			sampleTotal += boolScore(*s.RequiredKeywordsPresent)
			sampleCount++
		}
		if s.MustBeJSON || looksLikeJSON(s.Actual) {
			sampleTotal += JSONValidityScore(s.Actual)
			sampleCount++
		}
		if len(s.ExpectedFields) > 0 || len(s.ActualFields) > 0 {
			sampleTotal += FieldF1Score(s.ExpectedFields, s.ActualFields)
			sampleCount++
		}
		if sampleCount == 0 {
			continue
		}
		total += sampleTotal / float64(sampleCount)
		count++
	}
	if count == 0 {
		return 0, false
	}
	return clamp100(total / float64(count)), true
}

// ReliabilityScore implements 100*(1-errorRate-timeoutRate-invalidJSONRate).
func ReliabilityScore(t Telemetry) (float64, bool) {
	if t.Requests <= 0 {
		return 0, false
	}
	bad := t.Errors + t.Timeouts + t.InvalidJSON
	return clamp100(100 * (1 - float64(bad)/float64(t.Requests))), true
}

// LatencyScore maps threshold/2 to 100, threshold to 50 and 2*threshold to 0.
func LatencyScore(t Telemetry, thresholdMs float64) (float64, bool) {
	if !t.LatencyObserved || t.LatencyMillis <= 0 || thresholdMs <= 0 {
		return 0, false
	}
	half := thresholdMs / 2
	double := thresholdMs * 2
	switch {
	case t.LatencyMillis <= half:
		return 100, true
	case t.LatencyMillis >= double:
		return 0, true
	case t.LatencyMillis <= thresholdMs:
		return clamp100(100 - ((t.LatencyMillis-half)/(thresholdMs-half))*50), true
	default:
		return clamp100(50 - ((t.LatencyMillis-thresholdMs)/(double-thresholdMs))*50), true
	}
}

// SemanticScore averages injected local semantic scores.
func SemanticScore(samples []EvidenceSample) (float64, bool) {
	return averageOptional(samples, func(s EvidenceSample) *float64 { return s.SemanticScore })
}

// JudgedScore averages injected sovereign judge scores.
func JudgedScore(samples []EvidenceSample) (float64, bool) {
	return averageOptional(samples, func(s EvidenceSample) *float64 { return s.JudgedScore })
}

// ExactMatchScore is exact match after text normalization.
func ExactMatchScore(expected, actual string) float64 {
	if normalizeText(expected) == normalizeText(actual) {
		return 100
	}
	return 0
}

// ReferenceCorrectnessScore scores a natural-language answer against a golden
// reference without requiring byte-for-byte or phrase-order identity.
func ReferenceCorrectnessScore(reference, candidate string) float64 {
	if ExactMatchScore(reference, candidate) == 100 {
		return 100
	}
	return clamp100(
		0.60*ContentTokenCoverageScore(reference, candidate) +
			0.25*TokenF1Score(reference, candidate) +
			0.15*RougeLScore(reference, candidate),
	)
}

// SemanticSimilarityScore is a local deterministic semantic proxy for golden
// datasets where the reference captures the concepts expected in the answer.
func SemanticSimilarityScore(reference, candidate string) float64 {
	if ExactMatchScore(reference, candidate) == 100 {
		return 100
	}
	return clamp100(
		0.70*ContentTokenCoverageScore(reference, candidate) +
			0.20*TokenF1Score(reference, candidate) +
			0.10*RougeLScore(reference, candidate),
	)
}

// ContentTokenCoverageScore returns how many meaningful reference tokens are
// present in the candidate, which is stable for short reference answers.
func ContentTokenCoverageScore(reference, candidate string) float64 {
	ref := contentTokens(reference)
	cand := contentTokens(candidate)
	if len(ref) == 0 && len(cand) == 0 {
		return 100
	}
	if len(ref) == 0 || len(cand) == 0 {
		return 0
	}
	var overlap int
	for _, token := range uniqueTokens(ref) {
		if containsSimilarToken(cand, token) {
			overlap++
		}
	}
	return clamp100(100 * float64(overlap) / float64(len(uniqueTokens(ref))))
}

// TokenF1Score computes a set-token F1 over meaningful tokens.
func TokenF1Score(reference, candidate string) float64 {
	ref := uniqueTokens(contentTokens(reference))
	cand := uniqueTokens(contentTokens(candidate))
	if len(ref) == 0 && len(cand) == 0 {
		return 100
	}
	if len(ref) == 0 || len(cand) == 0 {
		return 0
	}
	var overlap int
	matched := make([]bool, len(cand))
	for _, token := range ref {
		for i, candidateToken := range cand {
			if matched[i] || !similarToken(token, candidateToken) {
				continue
			}
			matched[i] = true
			overlap++
			break
		}
	}
	precision := float64(overlap) / float64(len(cand))
	recall := float64(overlap) / float64(len(ref))
	if precision+recall == 0 {
		return 0
	}
	return clamp100(100 * (2 * precision * recall / (precision + recall)))
}

// EditSimilarityScore returns a normalized Levenshtein similarity in [0,100].
func EditSimilarityScore(a, b string) float64 {
	ra, rb := []rune(normalizeText(a)), []rune(normalizeText(b))
	maxLen := len(ra)
	if len(rb) > maxLen {
		maxLen = len(rb)
	}
	if maxLen == 0 {
		return 100
	}
	return clamp100(100 * (1 - float64(EditDistance(a, b))/float64(maxLen)))
}

// EditDistance computes Levenshtein distance over normalized runes.
func EditDistance(a, b string) int {
	ra, rb := []rune(normalizeText(a)), []rune(normalizeText(b))
	if len(ra) == 0 {
		return len(rb)
	}
	if len(rb) == 0 {
		return len(ra)
	}
	prev := make([]int, len(rb)+1)
	curr := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		curr[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 0
			if ra[i-1] != rb[j-1] {
				cost = 1
			}
			curr[j] = min3(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[len(rb)]
}

// RougeLScore computes an LCS-based ROUGE-L F1 in [0,100].
func RougeLScore(reference, candidate string) float64 {
	ref := tokenize(reference)
	cand := tokenize(candidate)
	if len(ref) == 0 && len(cand) == 0 {
		return 100
	}
	if len(ref) == 0 || len(cand) == 0 {
		return 0
	}
	lcs := lcsLen(ref, cand)
	precision := float64(lcs) / float64(len(cand))
	recall := float64(lcs) / float64(len(ref))
	if precision+recall == 0 {
		return 0
	}
	return clamp100(100 * (2 * precision * recall / (precision + recall)))
}

// JSONValidityScore returns 100 when raw is valid JSON, otherwise 0.
func JSONValidityScore(raw string) float64 {
	var v any
	if json.Unmarshal([]byte(raw), &v) == nil {
		return 100
	}
	return 0
}

// FieldF1Score computes exact normalized field-value F1.
func FieldF1Score(expected, actual map[string]string) float64 {
	if len(expected) == 0 && len(actual) == 0 {
		return 100
	}
	if len(expected) == 0 || len(actual) == 0 {
		return 0
	}
	var tp int
	for k, exp := range expected {
		if got, ok := actual[k]; ok && normalizeText(got) == normalizeText(exp) {
			tp++
		}
	}
	precision := float64(tp) / float64(len(actual))
	recall := float64(tp) / float64(len(expected))
	if precision+recall == 0 {
		return 0
	}
	return clamp100(100 * (2 * precision * recall / (precision + recall)))
}

func averageOptional(samples []EvidenceSample, pick func(EvidenceSample) *float64) (float64, bool) {
	var total float64
	var count int
	for _, s := range samples {
		v := pick(s)
		if v == nil {
			continue
		}
		total += clamp100(*v)
		count++
	}
	if count == 0 {
		return 0, false
	}
	return total / float64(count), true
}

func prefixSignals(prefix string, in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = append(out, prefix+": "+s)
	}
	return out
}

func looksLikeJSON(raw string) bool {
	raw = strings.TrimSpace(raw)
	return strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[")
}

func normalizeText(s string) string {
	return strings.Join(tokenize(s), " ")
}

func tokenize(s string) []string {
	s = foldLatin(strings.ToLower(strings.TrimSpace(s)))
	if s == "" {
		return nil
	}
	var b strings.Builder
	lastSpace := true
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.Fields(b.String())
}

func contentTokens(s string) []string {
	tokens := tokenize(s)
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if len([]rune(token)) <= 2 || stopword(token) {
			continue
		}
		out = append(out, token)
	}
	if len(out) > 0 {
		return out
	}
	return tokens
}

func uniqueTokens(tokens []string) []string {
	seen := make(map[string]bool, len(tokens))
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if seen[token] {
			continue
		}
		seen[token] = true
		out = append(out, token)
	}
	return out
}

func containsSimilarToken(tokens []string, want string) bool {
	for _, token := range tokens {
		if similarToken(want, token) {
			return true
		}
	}
	return false
}

func similarToken(a, b string) bool {
	if a == b {
		return true
	}
	if len(a) < 4 || len(b) < 4 {
		return false
	}
	return strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
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

func stopword(token string) bool {
	switch token {
	case "the", "and", "for", "with", "that", "this", "from", "into", "are", "was", "were", "has", "have", "had", "should", "must", "answer", "mention", "mentions", "a", "an", "of", "to", "in", "on", "at", "by", "or", "is", "be", "as", "le", "la", "les", "des", "une", "un", "du", "de", "et", "ou", "est", "sont", "dans", "pour", "avec", "sur", "par", "que", "qui", "aux", "au", "doit", "mentionner":
		return true
	default:
		return false
	}
}

func boolScore(v bool) float64 {
	if v {
		return 100
	}
	return 0
}

func lcsLen(a, b []string) int {
	dp := make([][]int, len(a)+1)
	for i := range dp {
		dp[i] = make([]int, len(b)+1)
	}
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else if dp[i-1][j] >= dp[i][j-1] {
				dp[i][j] = dp[i-1][j]
			} else {
				dp[i][j] = dp[i][j-1]
			}
		}
	}
	return dp[len(a)][len(b)]
}

func clamp100(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func min3(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}
