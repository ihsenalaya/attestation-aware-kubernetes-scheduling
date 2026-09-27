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
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

// moneyQuantity converts a float monetary amount to a resource.Quantity with
// micro-currency precision. Demo and per-request LLM costs are often below one
// cent; rounding to cents would turn real measured spend into a misleading zero.
func moneyQuantity(v float64) resource.Quantity {
	return resource.MustParse(fmt.Sprintf("%.6f", v))
}

// moneyQuantityPtr is the pointer form for optional status fields.
func moneyQuantityPtr(v float64) *resource.Quantity {
	q := moneyQuantity(v)
	return &q
}
