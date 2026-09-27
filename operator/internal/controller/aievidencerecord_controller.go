package controller

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
)

type AIEvidenceRecordReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aievidencerecords,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aievidencerecords/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiops.imperium.io,resources=aievidencerecords/finalizers,verbs=update

func (r *AIEvidenceRecordReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var record aiopsv1alpha1.AIEvidenceRecord
	if err := r.Get(ctx, req.NamespacedName, &record); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	record.Status.ObservedGeneration = record.Generation
	raw, _ := json.Marshal(record.Spec)
	record.Status.ChainDigest = platformcrypto.SHA256Hex(raw)
	record.Status.Anchored = record.Spec.ExternalCheckpointRef != ""
	if record.Status.Anchored {
		meta.SetStatusCondition(&record.Status.Conditions, readyTrue(record.Generation, "Evidence record chained and externally anchored"))
	} else {
		meta.SetStatusCondition(&record.Status.Conditions, readyTrue(record.Generation, "Evidence record chained"))
	}
	if err := r.Status().Update(ctx, &record); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *AIEvidenceRecordReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiopsv1alpha1.AIEvidenceRecord{}).
		Complete(r)
}
