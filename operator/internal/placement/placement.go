package placement

import "strings"

type Evidence struct {
	TEE              string
	AgeSeconds       int32
	Revoked          bool
	RuntimeClassName string
}

type Policy struct {
	AllowedTEEs           []string
	MaxEvidenceAgeSeconds int32
	RequiredRuntimeClass  string
}

type Decision struct {
	Allow   bool
	Reason  string
	Score   int
	Runtime string
}

func Evaluate(policy Policy, evidence Evidence) Decision {
	if evidence.Revoked {
		return Decision{Reason: "evidence revoked"}
	}
	if policy.MaxEvidenceAgeSeconds > 0 && evidence.AgeSeconds > policy.MaxEvidenceAgeSeconds {
		return Decision{Reason: "evidence expired"}
	}
	if len(policy.AllowedTEEs) > 0 {
		matched := false
		for _, tee := range policy.AllowedTEEs {
			if strings.EqualFold(strings.TrimSpace(tee), strings.TrimSpace(evidence.TEE)) {
				matched = true
				break
			}
		}
		if !matched {
			return Decision{Reason: "TEE not allowed"}
		}
	}
	if policy.RequiredRuntimeClass != "" && policy.RequiredRuntimeClass != evidence.RuntimeClassName {
		return Decision{Reason: "runtime class mismatch"}
	}
	score := 100
	if policy.MaxEvidenceAgeSeconds > 0 && evidence.AgeSeconds > 0 {
		score -= int((100 * evidence.AgeSeconds) / policy.MaxEvidenceAgeSeconds)
		if score < 1 {
			score = 1
		}
	}
	return Decision{Allow: true, Reason: "placement allowed", Score: score, Runtime: evidence.RuntimeClassName}
}
