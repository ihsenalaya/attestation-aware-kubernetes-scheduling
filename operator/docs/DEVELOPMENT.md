# Guide de développement

## Prérequis
Go 1.25, Docker, kubectl, kind, helm. Kubebuilder 3.14 (pour `create api`).

## Boucle courante
```bash
make manifests generate   # régénère CRDs, RBAC, deepcopy après modif des types/markers
make build                # compile le manager
make test                 # tests unitaires + envtest (télécharge les binaires k8s 1.31)
go test ./internal/...    # tests moteurs purs (rapides, sans envtest)
make run                  # exécute le manager localement contre le cluster courant
```

## Organisation
- Types CRD : `api/v1alpha1/*_types.go` (+ `common.go` pour conditions/Secret partagés).
- Controllers : `internal/controller/*_controller.go` (+ `status.go`, `money.go`, `catalog.go`).
- Moteurs **purs** (testables sans k8s) : `internal/{costengine,budgetengine,sovereigntyengine,breakevenengine,recommendationengine,enforcementengine,reporting}`.
- Collecte : `internal/collectors/{aigw,prometheus,configmap,fake}` (mode `litellm` retiré).
- Métriques : `internal/metrics`.

## Conventions
- Toute logique métier va dans un moteur **pur** ; le controller orchestre (collecte → moteur → status/metrics/events).
- Status : `observedGeneration` + condition standard `Ready` via `meta.SetStatusCondition`.
- Montants en `resource.Quantity` côté API ; `float64` dans les moteurs ; conversion via `money.go`.
- Après modif des markers `+kubebuilder:rbac` : `make manifests` (et répercuter dans le chart si besoin).

## Notes d'environnement
controller-gen **v0.18.0**, kustomize **v5.4.3**, envtest k8s **1.31.0** (bumpés pour Go 1.25 ;
les versions Kubebuilder 3.14 par défaut ne compilent pas sous Go 1.25).
