package bootstrap

import (
	"context"
	"fmt"
	"os"
	"strings"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	platformModeEnv            = "AIOPS_PLATFORM_MODE"
	platformModeSimulatedKind  = "simulated-kind"
	simulatedRuntimeClassLabel = "ai.sovereign.io/simulated"
)

// EnsureSimulatedRuntimeClasses creates kind-safe runtime classes for local simulation mode.
func EnsureSimulatedRuntimeClasses(ctx context.Context, c client.Client) error {
	if c == nil || platformMode() != platformModeSimulatedKind {
		return nil
	}
	for _, runtimeClassName := range []string{"simulated-kata-qemu-tdx", "simulated-kata-qemu-snp"} {
		rc := &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName}}
		if _, err := controllerutil.CreateOrUpdate(ctx, c, rc, func() error {
			rc.Handler = "runc"
			if rc.Labels == nil {
				rc.Labels = map[string]string{}
			}
			rc.Labels[simulatedRuntimeClassLabel] = "true"
			return nil
		}); err != nil {
			return fmt.Errorf("upsert RuntimeClass %q: %w", runtimeClassName, err)
		}
	}
	return nil
}

func platformMode() string {
	mode := strings.TrimSpace(strings.ToLower(strings.ReplaceAll(os.Getenv(platformModeEnv), "_", "-")))
	switch mode {
	case "", "kind", platformModeSimulatedKind:
		return platformModeSimulatedKind
	default:
		return mode
	}
}
