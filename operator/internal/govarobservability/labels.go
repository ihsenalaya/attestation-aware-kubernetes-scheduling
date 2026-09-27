package govarobservability

func member(value string, allowed []string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func allDecisions() []string {
	return []string{"ADMIT", "QUEUE", "REJECT", "ABSTAIN", "REQUIRE_APPROVAL"}
}
func validDecision(value string) bool { return member(value, allDecisions()) }

func allMethods() []string {
	return []string{"none", "strict_provider_cap", "mean", "fixed_margin", "fixed_quantile", "adaptive_quantile", "govar_fixed_cohort"}
}
func validMethod(value string) bool { return member(value, allMethods()) }

func allStates() []string {
	return []string{
		"NEW", "RESERVED", "DISPATCH_PENDING", "DISPATCHED", "UNRESOLVED",
		"SETTLED_PROVISIONAL", "LATE_SETTLED_PROVISIONAL", "CORRECTED_PROVISIONAL",
		"FINALIZED", "LATE_FINALIZED", "CANCELED_UNBILLED", "FAILED_UNBILLED",
		"EXPIRED_UNDISPATCHED",
	}
}
func validState(value string) bool { return member(value, allStates()) }

func allReasons() []string {
	return []string{
		"highest_utility_feasible", "no_candidate_after_governance", "insufficient_reservation_evidence",
		"budget_unavailable", "duplicate_request", "duplicate_request_conflict", "duplicate_event",
		"reservation_not_found", "duplicate_settlement", "approval_required",
		"authoritative_unbilled_cancellation", "cancellation_delivery_ambiguous", "dispatch_claimed",
		"dispatch_delivered", "dispatch_delivery_ambiguous", "provisional_settlement", "late_settlement",
		"settlement_correction", "settlement_finalized", "invalid_transition", "authenticated_principal_mismatch",
		"reservation_exceeded", "policy_not_ready", "budget_target_mismatch", "workload_target_mismatch",
		"model_not_ready", "provider_unavailable", "model_not_routable", "governance_infeasible",
		"quality_observation_stale", "quality_below_minimum", "pricing_incomplete", "pricing_stale",
		"context_limit_exceeded", "latency_observation_unavailable", "latency_guardrail_exceeded",
		"strict_cap_unverified", "insufficient_calibration", "calibration_status_missing",
		"calibration_generation_mismatch", "calibration_digest_mismatch", "calibration_regime_mismatch",
		"calibration_support_insufficient", "calibration_stale", "calibration_drift_detected",
		"calibration_detector_unsupported", "input_token_bound_uncertain", "reservation_method_unknown",
		"budget_window_conflict", "provider_retries_disabled_unreserved", "expired_undispatched",
		"budget_window_rollover",
	}
}
func validReason(value string) bool { return member(value, allReasons()) }

func allBases() []string {
	return []string{
		"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_tokens", "request",
		"tool_call", "media_unit", "billable_second", "cancellation", "retry_attempt",
	}
}
func validBasis(value string) bool { return member(value, allBases()) }

func allWorkerKinds() []string {
	return []string{"expiry", "delivery-reconciliation", "outbox-repair", "calibration-drift", "audit-checkpoint"}
}
func validWorkerKind(value string) bool { return member(value, allWorkerKinds()) }

func allWorkerClaimResults() []string          { return []string{"claimed", "empty", "lease_conflict", "error"} }
func validWorkerClaimResult(value string) bool { return member(value, allWorkerClaimResults()) }

func allWorkerStates() []string {
	return []string{"ready", "leased", "retry", "dead_letter", "ambiguous"}
}
func validWorkerState(value string) bool { return member(value, allWorkerStates()) }

func allDetectors() []string          { return []string{"none", "coverage-gap", "psi", "ks", "adwin"} }
func validDetector(value string) bool { return member(value, allDetectors()) }

func allOverflowOperations() []string {
	return []string{"admission", "reservation", "settlement", "reconciliation", "snapshot"}
}
func validOverflowOperation(value string) bool { return member(value, allOverflowOperations()) }

func allPricingReasons() []string {
	return []string{
		"missing", "partial", "stale", "unknown_adapter", "duplicate_basis", "unknown_basis",
		"negative_price", "nonintegral_price", "arithmetic_overflow", "unbounded_response",
		"usage_missing", "unlisted_category",
	}
}
func validPricingReason(value string) bool { return member(value, allPricingReasons()) }

func allAuditResults() []string          { return []string{"pass", "fail", "error"} }
func validAuditResult(value string) bool { return member(value, allAuditResults()) }

func allUnresolvedReasons() []string {
	return []string{
		"delivery_ambiguous", "settlement_missing", "cancellation_ambiguous", "dead_letter",
		"telemetry_missing", "provider_timeout", "database_error",
	}
}
func validUnresolvedReason(value string) bool { return member(value, allUnresolvedReasons()) }
