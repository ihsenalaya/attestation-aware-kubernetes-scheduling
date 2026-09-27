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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiopsv1alpha1 "github.com/imperium/ai-sovereign-finops-operator/api/v1alpha1"
)

// readyCondition builds a standard "Ready" condition for any aiops CRD.
func readyCondition(generation int64, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               aiopsv1alpha1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}
}

// readyTrue is the common "everything reconciled" condition.
func readyTrue(generation int64, message string) metav1.Condition {
	return readyCondition(generation, metav1.ConditionTrue, aiopsv1alpha1.ReasonReconciled, message)
}

// readyFalse is the common "could not reconcile" condition.
func readyFalse(generation int64, reason, message string) metav1.Condition {
	return readyCondition(generation, metav1.ConditionFalse, reason, message)
}
