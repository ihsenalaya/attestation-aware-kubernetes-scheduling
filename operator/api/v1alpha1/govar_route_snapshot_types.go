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

package v1alpha1

// GOVARRouteSnapshot is the immutable model/provider/pricing/route identity
// committed atomically with a GOV-AR reservation. It contains no credentials
// or endpoint URLs.
type GOVARRouteSnapshot struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	ModelName string `json:"model_name"`
	// +kubebuilder:validation:MinLength=1
	ModelUID string `json:"model_uid"`
	// +kubebuilder:validation:Minimum=1
	ModelGeneration int64 `json:"model_generation"`
	// +kubebuilder:validation:MinLength=1
	ModelResourceVersion string `json:"model_resource_version"`
	// +kubebuilder:validation:MinLength=1
	ProviderName string `json:"provider_name"`
	// +kubebuilder:validation:MinLength=1
	ProviderUID string `json:"provider_uid"`
	// +kubebuilder:validation:Minimum=1
	ProviderGeneration int64 `json:"provider_generation"`
	// +kubebuilder:validation:MinLength=1
	ProviderResourceVersion string `json:"provider_resource_version"`
	// +kubebuilder:validation:MinLength=1
	PricingVersion string `json:"pricing_version"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	PricingComplianceHash string `json:"pricing_compliance_hash"`
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	RouteBindingName string `json:"route_binding_name"`
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`
	ProviderDeployment string `json:"provider_deployment"`
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:-]*$`
	Cluster string `json:"cluster"`
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:-]*$`
	Authority string `json:"authority"`
	// +kubebuilder:validation:Enum=openai-body;azure-deployment-path;anthropic-body;google-generate-path
	PathMode string `json:"path_mode"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	SnapshotHash string `json:"snapshot_hash"`
}
