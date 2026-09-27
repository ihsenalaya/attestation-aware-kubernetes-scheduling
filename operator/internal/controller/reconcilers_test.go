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

package controller

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarpricing"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/attestation/maa"
)

const testNamespace = "default"

func reqFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: testNamespace}}
}

func ptrFloat64(v float64) *float64 { return &v }

// seedEmptyTelemetry registers a REAL but empty telemetry source so reconcilers
// that read measured spend have a source to read (and find zero usage) rather than
// failing with NoTelemetrySource. It is deliberately NOT the fake collector: an
// AIGateway in configmap mode pointed at a ConfigMap whose usage.json is an empty
// JSON array — a genuine, verifiable "no traffic yet" reading. This keeps the
// suite honest (no fabricated numbers) while exercising the happy path.
func seedEmptyTelemetry(ctx context.Context, prefix string) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: prefix + "-usage", Namespace: testNamespace},
		Data:       map[string]string{"usage.json": "[]"},
	}
	Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, cm) })

	gw := &aiopsv1alpha1.AIGateway{
		ObjectMeta: metav1.ObjectMeta{Name: prefix + "-gw", Namespace: testNamespace},
		Spec: aiopsv1alpha1.AIGatewaySpec{
			Type:     "litellm",
			Endpoint: "http://unused.local",
			Telemetry: aiopsv1alpha1.TelemetrySpec{
				Mode:            aiopsv1alpha1.TelemetryModeConfigMap,
				SourceConfigMap: prefix + "-usage",
			},
		},
	}
	Expect(k8sClient.Create(ctx, gw)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, gw) })
}

var _ = Describe("aiops reconcilers", func() {
	ctx := context.Background()

	Context("AIProvider", func() {
		It("registers and becomes Ready", func() {
			now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
			observed, validUntil := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(time.Hour))
			provider := &aiopsv1alpha1.AIProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "prov-ready", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIProviderSpec{
					Type:          "azure-openai",
					DataResidency: "france",
					Managed:       true,
					Pricing: aiopsv1alpha1.ProviderPricing{
						Currency:                   "EUR",
						InputTokenPricePerMillion:  resource.MustParse("2.5"),
						OutputTokenPricePerMillion: resource.MustParse("10"),
						Version:                    "catalog-v1",
						ObservedAt:                 &observed,
						Completeness:               aiopsv1alpha1.ProviderPricingComplete,
					},
					GOVAR: &aiopsv1alpha1.AIProviderGOVARSpec{Pricing: &aiopsv1alpha1.AIProviderGOVARPricingSpec{Evidence: aiopsv1alpha1.AIProviderPricingEvidenceSpec{Mode: aiopsv1alpha1.ProviderEvidenceAdminAttested, SourceVersion: "catalog-v1", EvidenceSHA256: strings.Repeat("a", 64), ValidUntil: validUntil, AdapterVersion: govarpricing.CurrentAdapterVersion}, InapplicableBases: []aiopsv1alpha1.ProviderBillableBasis{aiopsv1alpha1.ProviderBasisCachedInputTokens, aiopsv1alpha1.ProviderBasisReasoningTokens, aiopsv1alpha1.ProviderBasisRequest, aiopsv1alpha1.ProviderBasisToolCall, aiopsv1alpha1.ProviderBasisMediaUnit, aiopsv1alpha1.ProviderBasisBillableSecond, aiopsv1alpha1.ProviderBasisCancellation, aiopsv1alpha1.ProviderBasisRetryAttempt}}},
				},
			}
			Expect(k8sClient.Create(ctx, provider)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, provider) })

			r := &AIProviderReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return now }}
			_, err := r.Reconcile(ctx, reqFor("prov-ready"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIProvider{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "prov-ready", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Status.ObservedGeneration).To(Equal(got.Generation))
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
		})
	})

	Context("AIGateway", func() {
		It("resolves governed namespaces from the selector", func() {
			gw := &aiopsv1alpha1.AIGateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw-ready", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIGatewaySpec{
					Type:     "litellm",
					Endpoint: "http://litellm.default.svc.cluster.local:4000",
					NamespaceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"ai-finops/enabled": "true"},
					},
					Telemetry: aiopsv1alpha1.TelemetrySpec{Mode: aiopsv1alpha1.TelemetryModeFake},
				},
			}
			Expect(k8sClient.Create(ctx, gw)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, gw) })

			r := &AIGatewayReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("gw-ready"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIGateway{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "gw-ready", Namespace: testNamespace}, got)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
			// No namespace carries the label in the test env, so the set is empty (not nil-panic).
			Expect(got.Status.GovernedNamespaces).To(BeEmpty())
		})
	})

	Context("AIModel", func() {
		It("is NotReady when the provider is missing, Ready once it exists", func() {
			model := &aiopsv1alpha1.AIModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIModelSpec{
					ProviderRef: "prov-for-model",
					ModelName:   "mistral-small",
					Type:        "llm",
				},
			}
			Expect(k8sClient.Create(ctx, model)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, model) })

			r := &AIModelReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("model-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIModel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "model-x", Namespace: testNamespace}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, aiopsv1alpha1.ConditionReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiopsv1alpha1.ReasonReferenceNotFound))

			By("creating the referenced provider and reconciling again")
			provider := &aiopsv1alpha1.AIProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "prov-for-model", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIProviderSpec{
					Type: "self-hosted",
					Pricing: aiopsv1alpha1.ProviderPricing{
						Currency:                   "EUR",
						InputTokenPricePerMillion:  resource.MustParse("0"),
						OutputTokenPricePerMillion: resource.MustParse("0"),
					},
				},
			}
			Expect(k8sClient.Create(ctx, provider)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, provider) })

			_, err = r.Reconcile(ctx, reqFor("model-x"))
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "model-x", Namespace: testNamespace}, got)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
			Expect(got.Status.ResolvedProvider).To(Equal("self-hosted"))
		})
	})

	Context("AIBudgetPolicy", func() {
		It("evaluates spend against budget and becomes Ready", func() {
			seedEmptyTelemetry(ctx, "budget-x")
			policy := &aiopsv1alpha1.AIBudgetPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "budget-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIBudgetPolicySpec{
					Target:                   aiopsv1alpha1.BudgetTarget{Namespace: "rh"},
					Period:                   "monthly",
					BudgetEUR:                resource.MustParse("500"),
					WarningThresholdPercent:  70,
					CriticalThresholdPercent: 90,
					HardLimitPercent:         100,
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, policy) })

			r := &AIBudgetPolicyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("budget-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIBudgetPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "budget-x", Namespace: testNamespace}, got)).To(Succeed())
			// No catalog/spend in the test env → 0% of a 500 EUR budget.
			Expect(got.Status.Phase).To(Equal("WithinBudget"))
			Expect(got.Status.UsagePercent).To(Equal(int32(0)))
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
		})
	})

	Context("AISovereigntyPolicy", func() {
		It("registers in reportOnly mode and becomes Ready", func() {
			seedEmptyTelemetry(ctx, "sov-x")
			policy := &aiopsv1alpha1.AISovereigntyPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "sov-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AISovereigntyPolicySpec{
					DataResidency:   aiopsv1alpha1.DataResidencyRule{AllowedZones: []string{"FR", "EU"}},
					EnforcementMode: aiopsv1alpha1.EnforcementReportOnly,
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, policy) })

			r := &AISovereigntyPolicyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("sov-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AISovereigntyPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "sov-x", Namespace: testNamespace}, got)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
		})
	})

	Context("AIBreakEvenAnalysis", func() {
		It("computes a recommendation and becomes Ready", func() {
			seedEmptyTelemetry(ctx, "be-x")
			analysis := &aiopsv1alpha1.AIBreakEvenAnalysis{
				ObjectMeta: metav1.ObjectMeta{Name: "be-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIBreakEvenAnalysisSpec{
					Target:          aiopsv1alpha1.BreakEvenTarget{Namespace: "rh"},
					CurrentModelRef: "mistral-small",
					AlternativeSelfHosted: aiopsv1alpha1.SelfHostedOption{
						ModelName:         "llama-3-70b",
						Runtime:           "vllm",
						GpuCount:          2,
						MonthlyGpuCostEUR: resource.MustParse("1800"),
					},
					AnalysisWindowDays: 30,
				},
			}
			Expect(k8sClient.Create(ctx, analysis)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, analysis) })

			r := &AIBreakEvenAnalysisReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("be-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIBreakEvenAnalysis{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "be-x", Namespace: testNamespace}, got)).To(Succeed())
			// No catalog/usage in the test env → managed cost 0 vs self-hosted > 0,
			// so self-hosting yields no saving and the engine recommends keep-managed.
			Expect(got.Status.Recommendation).To(Equal(aiopsv1alpha1.RecommendationKeepManaged))
			Expect(got.Status.SelfHostedMonthlyCostEUR).NotTo(BeNil())
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
		})
	})

	Context("AttestationEvidence refresh", func() {
		It("requeues simulated evidence to keep it fresh, but not real evidence", func() {
			simEvidence := &aiopsv1alpha1.AttestationEvidence{
				ObjectMeta: metav1.ObjectMeta{Name: "ae-sim-refresh", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AttestationEvidenceSpec{
					SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "node-sim"},
					EvidenceType: "cpu",
					TEE:          "TDX",
					Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 300, Simulated: true},
					Simulated:    true,
				},
			}
			Expect(k8sClient.Create(ctx, simEvidence)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, simEvidence) })

			evr := &AttestationEvidenceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			res, err := evr.Reconcile(ctx, reqFor("ae-sim-refresh"))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0), "simulated evidence must be requeued for refresh")

			got := &aiopsv1alpha1.AttestationEvidence{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ae-sim-refresh", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Status.Verified).To(BeTrue())
			Expect(got.Status.LastVerifiedTime).NotTo(BeNil())

			// Real evidence (digest-backed, not simulated) must NOT be auto-refreshed.
			realEvidence := &aiopsv1alpha1.AttestationEvidence{
				ObjectMeta: metav1.ObjectMeta{Name: "ae-real-norefresh", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AttestationEvidenceSpec{
					SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "node-real"},
					EvidenceType: "cpu",
					TEE:          "SEV-SNP",
					Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 300},
					Digest:       "sha256:real-evidence-digest",
				},
			}
			Expect(k8sClient.Create(ctx, realEvidence)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, realEvidence) })

			resReal, err := evr.Reconcile(ctx, reqFor("ae-real-norefresh"))
			Expect(err).NotTo(HaveOccurred())
			Expect(resReal.RequeueAfter).To(BeZero(), "real evidence freshness must not be auto-refreshed")
		})
	})

	Context("RawAttestationReport central verifier", func() {
		It("turns a simulated raw report into simulated verified evidence", func() {
			report := &aiopsv1alpha1.RawAttestationReport{
				ObjectMeta: metav1.ObjectMeta{Name: "raw-sim-report", Namespace: testNamespace},
				Spec: aiopsv1alpha1.RawAttestationReportSpec{
					NodeName:            "node-sim-report",
					NodeUID:             "node-uid-sim",
					Provider:            "simulator",
					RawToken:            "simulated-token",
					RawTokenHash:        "simulated-token-hash",
					Nonce:               "nonce-sim",
					CollectedAt:         metav1.Now(),
					AgentPodUID:         "agent-pod-sim",
					AgentServiceAccount: "node-attestation-agent",
					Simulated:           true,
				},
			}
			Expect(k8sClient.Create(ctx, report)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, report) })
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, &aiopsv1alpha1.AttestationEvidence{
					ObjectMeta: metav1.ObjectMeta{Name: "evidence-node-sim-report", Namespace: testNamespace},
				})
			})

			r := &RawAttestationReportReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				VerifierIdentity: "test-central-verifier",
				VerifierPodUID:   "verifier-pod-sim",
				ExpectedTEE:      "sevsnpvm",
			}
			res, err := r.Reconcile(ctx, reqFor("raw-sim-report"))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			evidence := &aiopsv1alpha1.AttestationEvidence{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "evidence-node-sim-report", Namespace: testNamespace}, evidence)).To(Succeed())
			Expect(evidence.Spec.Simulated).To(BeTrue())
			Expect(evidence.Status.Verified).To(BeTrue())
			Expect(evidence.Status.EvidenceMode).To(Equal(aiopsv1alpha1.EvidenceModeSimulated))
			Expect(evidence.Status.VerificationStatus).To(Equal(aiopsv1alpha1.VerificationStatusVerified))
			Expect(evidence.Status.VerifiedBy).To(Equal("test-central-verifier"))
			Expect(meta.IsStatusConditionTrue(evidence.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())

			gotReport := &aiopsv1alpha1.RawAttestationReport{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "raw-sim-report", Namespace: testNamespace}, gotReport)).To(Succeed())
			Expect(gotReport.Status.Processed).To(BeTrue())
			Expect(gotReport.Status.EvidenceRef).To(Equal("evidence-node-sim-report"))
			Expect(gotReport.Status.VerificationStatus).To(Equal(aiopsv1alpha1.VerificationStatusVerified))
		})

		It("keeps a failed real raw report unverified", func() {
			report := &aiopsv1alpha1.RawAttestationReport{
				ObjectMeta: metav1.ObjectMeta{Name: "raw-real-fail-report", Namespace: testNamespace},
				Spec: aiopsv1alpha1.RawAttestationReportSpec{
					NodeName:            "node-real-fail-report",
					NodeUID:             "node-uid-real",
					Provider:            "maa",
					RawToken:            "not-a-valid-maa-token",
					RawTokenHash:        "raw-token-hash",
					Nonce:               "nonce-real",
					CollectedAt:         metav1.Now(),
					AgentPodUID:         "agent-pod-real",
					AgentServiceAccount: "node-attestation-agent",
					Simulated:           false,
				},
			}
			Expect(k8sClient.Create(ctx, report)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, report) })
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, &aiopsv1alpha1.AttestationEvidence{
					ObjectMeta: metav1.ObjectMeta{Name: "evidence-node-real-fail-report", Namespace: testNamespace},
				})
			})

			r := &RawAttestationReportReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				VerifierIdentity: "test-central-verifier",
				VerifierPodUID:   "verifier-pod-real",
				ExpectedTEE:      "sevsnpvm",
				verifyFn: func(token string, opts maa.Options) maa.Result {
					Expect(token).To(Equal("not-a-valid-maa-token"))
					Expect(opts.ExpectedNonce).To(Equal("nonce-real"))
					return maa.Result{
						Status:    maa.StatusFailed,
						Reason:    "injected verifier failure",
						TokenHash: "sha256:bogus",
					}
				},
			}
			res, err := r.Reconcile(ctx, reqFor("raw-real-fail-report"))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())

			evidence := &aiopsv1alpha1.AttestationEvidence{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "evidence-node-real-fail-report", Namespace: testNamespace}, evidence)).To(Succeed())
			Expect(evidence.Spec.Simulated).To(BeFalse())
			Expect(evidence.Status.Verified).To(BeFalse())
			Expect(evidence.Status.EvidenceMode).To(Equal(aiopsv1alpha1.EvidenceModeUnverified))
			Expect(evidence.Status.VerificationStatus).To(Equal(aiopsv1alpha1.VerificationStatusFailed))
			Expect(evidence.Status.FailureReason).To(Equal("injected verifier failure"))
			Expect(evidence.Status.MAATokenHash).To(Equal("sha256:bogus"))
			Expect(meta.IsStatusConditionFalse(evidence.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())

			gotReport := &aiopsv1alpha1.RawAttestationReport{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "raw-real-fail-report", Namespace: testNamespace}, gotReport)).To(Succeed())
			Expect(gotReport.Status.Processed).To(BeTrue())
			Expect(gotReport.Status.EvidenceRef).To(Equal("evidence-node-real-fail-report"))
			Expect(gotReport.Status.VerificationStatus).To(Equal(aiopsv1alpha1.VerificationStatusFailed))
		})
	})

	Context("AIPlacementDecision", func() {
		It("allows placement with verified non-revoked evidence", func() {
			policy := &aiopsv1alpha1.ConfidentialInferencePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "conf-place", Namespace: testNamespace},
				Spec: aiopsv1alpha1.ConfidentialInferencePolicySpec{
					Target:                aiopsv1alpha1.WorkloadTarget{},
					RequiredTEE:           []string{"TDX"},
					MaxEvidenceAgeSeconds: 300,
					AllowedRuntimeClasses: []string{"simulated-kata-qemu-tdx"},
					EnforcementMode:       aiopsv1alpha1.EnforcementModeEnforce,
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, policy) })

			evidence := &aiopsv1alpha1.AttestationEvidence{
				ObjectMeta: metav1.ObjectMeta{Name: "ae-place", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AttestationEvidenceSpec{
					SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "risk-assistant"},
					EvidenceType: "runtime",
					TEE:          "TDX",
					Runtime:      aiopsv1alpha1.RuntimeExpectation{RuntimeClassName: "simulated-kata-qemu-tdx", Simulated: true},
					Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 30, Simulated: true},
					Simulated:    true,
				},
			}
			Expect(k8sClient.Create(ctx, evidence)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, evidence) })

			evr := &AttestationEvidenceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := evr.Reconcile(ctx, reqFor("ae-place"))
			Expect(err).NotTo(HaveOccurred())

			placementDecision := &aiopsv1alpha1.AIPlacementDecision{
				ObjectMeta: metav1.ObjectMeta{Name: "place-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIPlacementDecisionSpec{
					TargetRef:      aiopsv1alpha1.ObjectReference{Name: "risk-assistant"},
					PolicyRef:      aiopsv1alpha1.ObjectReference{Name: "conf-place"},
					EvidenceRef:    &aiopsv1alpha1.ObjectReference{Name: "ae-place"},
					PlacementToken: aiopsv1alpha1.PlacementTokenSpec{Required: true, TTLSeconds: 300},
					SchedulerName:  "ai-attestation-scheduler",
				},
			}
			Expect(k8sClient.Create(ctx, placementDecision)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, placementDecision) })

			r := &AIPlacementDecisionReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err = r.Reconcile(ctx, reqFor("place-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIPlacementDecision{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "place-x", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Status.Decision).To(Equal("allow"))
			Expect(got.Status.PlacementTokenDigest).NotTo(BeEmpty())
		})
	})

	Context("AIKeyReleasePolicy", func() {
		It("allows key release with verified evidence and valid placement", func() {
			policy := &aiopsv1alpha1.ConfidentialInferencePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "conf-kr", Namespace: testNamespace},
				Spec: aiopsv1alpha1.ConfidentialInferencePolicySpec{
					Target:                aiopsv1alpha1.WorkloadTarget{},
					RequiredTEE:           []string{"TDX"},
					MaxEvidenceAgeSeconds: 300,
					AllowedRuntimeClasses: []string{"simulated-kata-qemu-tdx"},
					EnforcementMode:       aiopsv1alpha1.EnforcementModeEnforce,
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, policy) })

			evidence := &aiopsv1alpha1.AttestationEvidence{
				ObjectMeta: metav1.ObjectMeta{Name: "ae-kr", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AttestationEvidenceSpec{
					SubjectRef:   aiopsv1alpha1.ObjectReference{Name: "risk-assistant"},
					EvidenceType: "runtime",
					TEE:          "TDX",
					Runtime:      aiopsv1alpha1.RuntimeExpectation{RuntimeClassName: "simulated-kata-qemu-tdx", Simulated: true},
					Freshness:    aiopsv1alpha1.EvidenceFreshness{MaxAgeSeconds: 30, Simulated: true},
					Simulated:    true,
				},
			}
			Expect(k8sClient.Create(ctx, evidence)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, evidence) })
			evr := &AttestationEvidenceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := evr.Reconcile(ctx, reqFor("ae-kr"))
			Expect(err).NotTo(HaveOccurred())

			placementDecision := &aiopsv1alpha1.AIPlacementDecision{
				ObjectMeta: metav1.ObjectMeta{Name: "place-kr", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIPlacementDecisionSpec{
					TargetRef:      aiopsv1alpha1.ObjectReference{Name: "risk-assistant"},
					PolicyRef:      aiopsv1alpha1.ObjectReference{Name: "conf-kr"},
					EvidenceRef:    &aiopsv1alpha1.ObjectReference{Name: "ae-kr"},
					PlacementToken: aiopsv1alpha1.PlacementTokenSpec{Required: true, TTLSeconds: 300},
				},
			}
			Expect(k8sClient.Create(ctx, placementDecision)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, placementDecision) })
			pdr := &AIPlacementDecisionReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err = pdr.Reconcile(ctx, reqFor("place-kr"))
			Expect(err).NotTo(HaveOccurred())

			keyPolicy := &aiopsv1alpha1.AIKeyReleasePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "kr-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIKeyReleasePolicySpec{
					EvidenceRef:             &aiopsv1alpha1.ObjectReference{Name: "ae-kr"},
					RequireAttestedEvidence: true,
					KeyRelease:              aiopsv1alpha1.PolicyKeyReleaseSpec{Required: true, TTLSeconds: 300},
					EnforcementMode:         aiopsv1alpha1.EnforcementModeEnforce,
				},
			}
			Expect(k8sClient.Create(ctx, keyPolicy)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, keyPolicy) })

			r := &AIKeyReleasePolicyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err = r.Reconcile(ctx, reqFor("kr-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIKeyReleasePolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "kr-x", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Status.LastDecision).To(Equal("allow"))
		})
	})

	Context("AIFinOpsReport", func() {
		It("stamps GeneratedAt and becomes Ready", func() {
			seedEmptyTelemetry(ctx, "report-x")
			report := &aiopsv1alpha1.AIFinOpsReport{
				ObjectMeta: metav1.ObjectMeta{Name: "report-x", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIFinOpsReportSpec{
					Target: aiopsv1alpha1.ReportTarget{Namespace: "rh", Period: "monthly"},
				},
			}
			Expect(k8sClient.Create(ctx, report)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, report) })

			r := &AIFinOpsReportReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("report-x"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIFinOpsReport{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "report-x", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Status.GeneratedAt).NotTo(BeNil())
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
		})
	})

	Context("AIQualityGate", func() {
		It("passes when dataset, evidence and telemetry satisfy the gate", func() {
			usage := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "qg-usage", Namespace: testNamespace},
				Data: map[string]string{"usage.json": `[
					{"namespace":"finance","application":"risk-assistant","team":"finance","provider":"azure-us","model":"gpt-us-mini","requests":10,"inputTokens":100,"outputTokens":200,"latencyMillis":1000,"errors":0},
					{"namespace":"finance","application":"risk-assistant","team":"finance","provider":"azure-fr","model":"gpt-france-mini","requests":10,"inputTokens":100,"outputTokens":200,"latencyMillis":900,"errors":0}
				]`},
			}
			Expect(k8sClient.Create(ctx, usage)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, usage) })

			gw := &aiopsv1alpha1.AIGateway{
				ObjectMeta: metav1.ObjectMeta{Name: "qg-gw", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIGatewaySpec{
					Type:     "litellm",
					Endpoint: "http://qg-gateway.test",
					Telemetry: aiopsv1alpha1.TelemetrySpec{
						Mode:            aiopsv1alpha1.TelemetryModeConfigMap,
						SourceConfigMap: "qg-usage",
					},
				},
			}
			Expect(k8sClient.Create(ctx, gw)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, gw) })

			dataset := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "finance-golden", Namespace: testNamespace},
				Data: map[string]string{"prompts.yaml": `
- id: risk-summary
  prompt: "Summarize supplier risk."
  expected:
    reference: "Supplier risk includes counterparty liquidity and operational exposure."
- id: var-explain
  prompt: "Explain value at risk."
  expected:
    reference: "Value at risk estimates portfolio loss at a confidence level."
`},
			}
			Expect(k8sClient.Create(ctx, dataset)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, dataset) })

			evidence := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "finance-evidence",
					Namespace: testNamespace,
					Labels: map[string]string{
						qualityGateLabel:           "finance-quality",
						qualityEvidenceSourceLabel: qualityEvidenceSourceJob,
					},
				},
				Data: map[string]string{"results.yaml": `
- id: risk-summary
  model: gpt-us-mini
  reference: "Supplier risk includes counterparty liquidity and operational exposure."
  actual: "Supplier risk includes counterparty liquidity and operational exposure."
  semanticScore: 96
  schemaValid: true
  unexpectedRefusal: false
  sensitiveDataLeak: false
  requiredKeywordsPresent: true
- id: var-explain
  model: gpt-us-mini
  reference: "Value at risk estimates portfolio loss at a confidence level."
  actual: "Value at risk estimates portfolio loss at a confidence level."
  semanticScore: 95
  schemaValid: true
  unexpectedRefusal: false
  sensitiveDataLeak: false
  requiredKeywordsPresent: true
- id: risk-summary
  model: gpt-france-mini
  reference: "Supplier risk includes counterparty liquidity and operational exposure."
  actual: "Supplier risk includes counterparty liquidity and operational exposure."
  semanticScore: 95
  schemaValid: true
  unexpectedRefusal: false
  sensitiveDataLeak: false
  requiredKeywordsPresent: true
- id: var-explain
  model: gpt-france-mini
  reference: "Value at risk estimates portfolio loss at a confidence level."
  actual: "Value at risk estimates portfolio loss at a confidence level."
  semanticScore: 94
  schemaValid: true
  unexpectedRefusal: false
  sensitiveDataLeak: false
  requiredKeywordsPresent: true
`},
			}
			Expect(k8sClient.Create(ctx, evidence)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, evidence) })

			gate := &aiopsv1alpha1.AIQualityGate{
				ObjectMeta: metav1.ObjectMeta{Name: "finance-quality", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIQualityGateSpec{
					Target:         aiopsv1alpha1.AIQualityGateTarget{Namespace: "finance", Application: "risk-assistant"},
					SourceModel:    "gpt-us-mini",
					CandidateModel: "gpt-france-mini",
					GoldenDatasetRef: aiopsv1alpha1.ConfigMapDataReference{
						Name: "finance-golden",
					},
					EvidenceRef:        &aiopsv1alpha1.ConfigMapDataReference{Name: "finance-evidence"},
					LatencyThresholdMs: 1500,
					MinSamples:         2,
					Weights: aiopsv1alpha1.AIQualityScoreWeights{
						Judged: ptrFloat64(0),
					},
					RequiredChecks: aiopsv1alpha1.AIQualityRequiredChecks{
						SchemaValid:               true,
						NoUnexpectedRefusal:       true,
						NoSensitiveDataLeak:       true,
						RequiredKeywords:          []string{"risk"},
						MaxErrorRatePercent:       2,
						MaxLatencyIncreasePercent: 20,
					},
				},
			}
			Expect(k8sClient.Create(ctx, gate)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, gate) })

			r := &AIQualityGateReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("finance-quality"))
			Expect(err).NotTo(HaveOccurred())
			_, err = r.Reconcile(ctx, reqFor("finance-quality"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIQualityGate{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "finance-quality", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(aiopsv1alpha1.AIQualityGatePassed))
			Expect(got.Status.Verdict).To(Equal("candidate-safe"))
			Expect(got.Status.CheckedSamples).To(Equal(int32(2)))
			Expect(got.Status.FailedChecks).To(Equal(int32(0)))
			Expect(got.Status.CandidateObservation).NotTo(BeNil())
			Expect(got.Status.QualityScore).To(BeNumerically(">", 0))
			Expect(got.Status.ScoreBreakdown.Correctness).To(BeNumerically(">", 0))
			Expect(got.Status.Dimensions).NotTo(BeEmpty())
			Expect(meta.IsStatusConditionTrue(got.Status.Conditions, aiopsv1alpha1.ConditionReady)).To(BeTrue())
		})

		It("cleans evaluation jobs and live evidence through the finalizer", func() {
			gate := &aiopsv1alpha1.AIQualityGate{
				ObjectMeta: metav1.ObjectMeta{Name: "cleanup-quality", Namespace: testNamespace},
				Spec: aiopsv1alpha1.AIQualityGateSpec{
					Target:           aiopsv1alpha1.AIQualityGateTarget{Namespace: "finance", Application: "risk-assistant"},
					SourceModel:      "gpt-us-mini",
					CandidateModel:   "gpt-france-mini",
					GoldenDatasetRef: aiopsv1alpha1.ConfigMapDataReference{Name: "cleanup-golden"},
					EvidenceRef:      &aiopsv1alpha1.ConfigMapDataReference{Name: "cleanup-evidence"},
				},
			}
			Expect(k8sClient.Create(ctx, gate)).To(Succeed())

			r := &AIQualityGateReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reqFor("cleanup-quality"))
			Expect(err).NotTo(HaveOccurred())

			got := &aiopsv1alpha1.AIQualityGate{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "cleanup-quality", Namespace: testNamespace}, got)).To(Succeed())
			Expect(got.Finalizers).To(ContainElement(qualityGateFinalizer))

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "quality-eval-cleanup",
					Namespace: testNamespace,
					Labels: map[string]string{
						qualityGateLabel:      "cleanup-quality",
						qualityEvaluatorLabel: "true",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "quality-evaluator",
								Image: "busybox:1.36",
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			evidence := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cleanup-evidence",
					Namespace: testNamespace,
					Labels: map[string]string{
						qualityGateLabel:           "cleanup-quality",
						qualityEvidenceSourceLabel: qualityEvidenceSourceJob,
					},
				},
				Data: map[string]string{"results.yaml": "samples: []"},
			}
			Expect(k8sClient.Create(ctx, evidence)).To(Succeed())

			Expect(k8sClient.Delete(ctx, got)).To(Succeed())
			_, err = r.Reconcile(ctx, reqFor("cleanup-quality"))
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				var job batchv1.Job
				err := k8sClient.Get(ctx, types.NamespacedName{Name: "quality-eval-cleanup", Namespace: testNamespace}, &job)
				return apierrors.IsNotFound(err) || !job.ObjectMeta.DeletionTimestamp.IsZero()
			}).Should(BeTrue())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: "cleanup-evidence", Namespace: testNamespace}, &corev1.ConfigMap{})
				return apierrors.IsNotFound(err)
			}).Should(BeTrue())
		})
	})

	Context("Gateway actuator", func() {
		It("ignores a missing Envoy route CRD only when there is no desired actuation", func() {
			got, err := actuateReroutesWithAnnotation(ctx, k8sClient, testNamespace, nil, budgetRerouteAnnotation)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeEmpty())

			_, err = actuateReroutesWithAnnotation(ctx, k8sClient, testNamespace, map[string]string{"gpt-4o": "mistral-small"}, budgetRerouteAnnotation)
			Expect(err).To(HaveOccurred())
			Expect(meta.IsNoMatchError(err)).To(BeTrue())
		})
	})
})
