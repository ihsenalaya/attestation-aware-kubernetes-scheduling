# Guide de contribution

Merci de contribuer à l'AI Sovereign FinOps Operator.

## Workflow
1. Crée une branche depuis `main`.
2. Implémente + ajoute des tests (moteur pur → test unitaire ; controller → envtest).
3. `make manifests generate fmt vet test` doivent passer.
4. Mets à jour la doc concernée (`docs/crds/`, `docs/features/`) et `docs/ROADMAP.md`.
5. Ouvre une PR avec une description claire (quoi/pourquoi) et la sortie des tests.

## Standards de code
- Go idiomatique, packages cohérents, commentaires sur les exports.
- Pas de logique métier dans les controllers : la mettre dans un moteur pur.
- Par défaut, rester **lecture seule** ; toute mutation du plan de données doit passer par un chemin
  d'enforcement explicite, testé et documenté.
- Garder le stack **CNCF/OSS** (pas de dépendance propriétaire).

## Tests
- Couverture cible : moteurs ≥ 80 %, controllers via envtest sur le flux Ready/erreurs.
- `go test ./internal/...` pour le retour rapide ; `make test` avant de pousser.

## Revue
- 1 approbation minimum ; CI verte (voir `.github/workflows`).
- Les changements d'API (`api/v1alpha1`) nécessitent `make manifests` committé et une note de migration.
