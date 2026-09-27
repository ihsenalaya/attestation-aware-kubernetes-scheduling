package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govar"
	"github.com/imperium/ai-sovereign-finops-operator/internal/govarcalibration"
)

type recordingCalibrationPublicationStore struct {
	software     string
	publications []govar.PolicyEvidencePublication
}

func (s *recordingCalibrationPublicationStore) SoftwareSHA256() string { return s.software }

func (s *recordingCalibrationPublicationStore) PublishPolicyEvidence(_ context.Context, publication govar.PolicyEvidencePublication) error {
	s.publications = append(s.publications, publication)
	return nil
}

func TestGOVARCalibrationStatusPublisherRejectsStaleResourceVersion(t *testing.T) {
	now := time.Date(2026, 7, 13, 14, 0, 0, 0, time.UTC)
	digest := func(value string) string {
		result, err := govarcalibration.CanonicalDigest("publisher-test-v1", value)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	regimes := govarcalibration.RegimeDigests{FeatureSHA256: digest("feature"), PriceSHA256: digest("price"),
		CapPathAdapterSHA256: digest("cap"), SplitOpportunitySHA256: digest("split"), CohortSHA256: digest("cohort"),
		ProducerSoftwareSHA256: digest("software")}
	rows := make([]govarcalibration.AuthoritativeObservation, 5)
	for i := range rows {
		rows[i] = govarcalibration.AuthoritativeObservation{RequestID: "publisher-row-" + string(rune('a'+i)),
			ProviderAttemptID: "publisher-attempt-" + string(rune('a'+i)), OpportunityID: "publisher-opportunity-" + string(rune('a'+i)),
			Split: govarcalibration.SplitCalibration, OutputTokens: int64(i + 1), SettledAt: now.Add(time.Duration(i) * time.Second), Regimes: regimes}
	}
	artifact, err := govarcalibration.BuildAuthoritativeArtifact(govarcalibration.AuthoritativeBuildConfig{
		ArtifactRef: "publisher-artifact", Version: "v1", FeatureSchemaVersion: "features-v1",
		CoverageTargetPPB: 800_000_000, MinimumSupport: 5, Regimes: regimes,
	}, rows)
	if err != nil {
		t.Fatal(err)
	}
	policy := &aiopsv1alpha1.AIRoutingPolicy{ObjectMeta: metav1.ObjectMeta{Name: "routing", Namespace: "finance",
		UID: types.UID("policy-uid"), Generation: 3, ResourceVersion: "10"}, Spec: aiopsv1alpha1.AIRoutingPolicySpec{
		GOVAR: &aiopsv1alpha1.GOVARRoutingPolicySpec{Calibration: &aiopsv1alpha1.GOVARCalibrationPolicy{
			ArtifactRef: artifact.ArtifactRef, ArtifactSHA256: artifact.ArtifactSHA256,
			CalibrationInputSHA256: artifact.SourceObservationsSHA256, RegistryID: "registry-v8",
			Version: artifact.Version, FeatureSchemaVersion: artifact.FeatureSchemaVersion,
			PriceRegimeSHA256: artifact.PriceRegimeSHA256, CapRegimeSHA256: artifact.CapRegimeSHA256,
			ProducerSoftwareSHA256: artifact.ProducerSoftwareSHA256, CoverageTargetPPB: artifact.CoverageTargetPPB,
			MinimumSupport: artifact.MinimumSupport, MaxAgeSeconds: 3600},
			Drift: aiopsv1alpha1.GOVARDriftPolicy{Detector: "coverage-gap", ThresholdPPB: 50_000_000,
				Fallback: "strict_provider_cap", RevalidationMinimumSupport: 5}}}}
	scheme := runtime.NewScheme()
	if err := aiopsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&aiopsv1alpha1.AIRoutingPolicy{}).WithObjects(policy).Build()
	store := &recordingCalibrationPublicationStore{software: regimes.ProducerSoftwareSHA256}
	publisher := GOVARCalibrationStatusPublisher{Client: client, Producer: store, Now: func() time.Time { return now }}
	base := CalibrationStatusPublicationRequest{TenantID: "tenant-a", Policy: types.NamespacedName{Namespace: policy.Namespace, Name: policy.Name},
		PolicyUID: policy.UID, PolicyGeneration: policy.Generation, Artifact: artifact}

	stale := base
	stale.ExpectedResourceVersion = "9"
	if err := publisher.Publish(context.Background(), stale); err == nil || !apierrors.IsConflict(err) {
		t.Fatalf("stale resourceVersion was not rejected with conflict: %v", err)
	}
	if len(store.publications) != 0 {
		t.Fatalf("stale writer reached immutable publication store: %+v", store.publications)
	}

	exact := base
	exact.ExpectedResourceVersion = "10"
	if err := publisher.Publish(context.Background(), exact); err != nil {
		t.Fatal(err)
	}
	if len(store.publications) != 1 || store.publications[0].ExpectedResourceVersion != "10" ||
		store.publications[0].PolicyUID != string(policy.UID) || !strings.EqualFold(store.publications[0].ProducerSoftwareSHA256, regimes.ProducerSoftwareSHA256) {
		t.Fatalf("publication did not commit exact CAS identity: %+v", store.publications)
	}
	var updated aiopsv1alpha1.AIRoutingPolicy
	if err := client.Get(context.Background(), base.Policy, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.GOVAR == nil || updated.Status.GOVAR.Calibration == nil ||
		updated.Status.GOVAR.Calibration.EvidenceSource != "postgresql-v8" ||
		updated.Status.GOVAR.Calibration.SourceResourceVersion != "10" ||
		updated.Status.GOVAR.Calibration.PublicationSHA256 != store.publications[0].PublicationSHA256 {
		t.Fatalf("published status lost authoritative identity: %+v", updated.Status.GOVAR)
	}
}
