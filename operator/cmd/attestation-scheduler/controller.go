package main

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/imperium/ai-sovereign-finops-operator/internal/scheduler"
)

// podScheduler reconciles pending pods that target the ai-attestation-scheduler.
type podScheduler struct {
	sched *scheduler.Scheduler
}

// Reconcile is called for each pending pod assigned to this scheduler.
func (r *podScheduler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pod corev1.Pod
	if err := r.sched.Client().Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if pod.Spec.SchedulerName != schedulerName {
		return ctrl.Result{}, nil
	}
	if pod.Status.Phase != corev1.PodPending || pod.Spec.NodeName != "" {
		return ctrl.Result{}, nil
	}
	// Skip pods that still have unresolved schedulingGates
	if len(pod.Spec.SchedulingGates) > 0 {
		logger.V(1).Info("pod has scheduling gates — deferring", "gates", pod.Spec.SchedulingGates)
		return ctrl.Result{}, nil
	}

	if err := r.sched.SchedulePod(ctx, &pod); err != nil {
		logger.Error(err, "schedule pod")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller with the manager.
func (r *podScheduler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}, builder.WithPredicates(
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return false
				}
				return pod.Spec.SchedulerName == schedulerName &&
					pod.Status.Phase == corev1.PodPending &&
					pod.Spec.NodeName == ""
			}),
		)).
		Complete(r)
}
