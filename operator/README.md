# AI Sovereign FinOps Operator

> **Plan de contrôle FinOps & souveraineté pour l'IA d'entreprise**, packagé comme opérateur Kubernetes
> natif. Il gouverne les appels LLM d'une organisation — **coût, attribution, fournisseurs, résidence
> des données, budgets et arbitrage API managée vs auto-hébergement** — en pur déclaratif (CRDs).

Stack 100 % **open-source / CNCF** : Kubernetes, controller-runtime, Prometheus, Grafana, Helm,
Kustomize, ArgoCD, Envoy. Apache 2.0 · Go 1.25 · API `aiops.imperium.io/v1alpha1`.

---

## 1. Le problème

Les entreprises adoptent les LLM plus vite qu'elles ne savent les gouverner. Trois angles morts
reviennent systématiquement dans les organisations régulées (santé, finance, assurance, secteur public,
industrie critique) :

| Problème | Sans l'opérateur | Avec l'opérateur |
|---|---|---|
| **Opacité des coûts** | personne ne sait quelle équipe/app dépense quoi, ni quand un modèle moins cher suffirait | coût attribué par modèle / équipe / namespace / application, exposé en métriques et rapports |
| **Souveraineté & conformité** | la résidence des données et l'usage de données sensibles sont gérés au cas par cas, voire pas du tout | politiques déclaratives de zones autorisées/interdites et de données sensibles, avec détection des violations |
| **Managé vs auto-hébergé** | aucune visibilité sur le volume à partir duquel l'auto-hébergement devient rentable | calcul du **point mort** (break-even) et recommandation à partir de l'usage réel |

L'opérateur transforme ces décisions ad hoc en **politiques Kubernetes versionnées, auditables et
réconciliées en continu**.

> ⚠️ **Le produit ne promet pas la conformité juridique.** Il produit une **traçabilité exploitable et
> un dossier de préparation à l'audit** (RGPD, AI Act, politiques internes). L'`enforcementMode` d'une
> `AISovereigntyPolicy` pilote la réaction : `reportOnly` (constat seul), `warn` (alerte différenciée —
> Events Kubernetes + métrique `ai_finops_enforcement_actions`, sans blocage), `enforce` (**agit
> réellement** dans le plan de données Envoy AI Gateway : reroute vers un backend conforme quand il existe,
> sinon blocage via le backend réservé absent `aiops-blocked`). C'est un vrai **plan de contrôle**, pas un
> simple observateur.

---

## 2. Comment ça marche

Le **plan de données** reste dans la gateway IA (Envoy / LiteLLM / Gateway API / custom). L'opérateur
est le **plan de contrôle** : il lit la télémétrie, applique les politiques déclarées en CRDs, et publie
coûts, constats de souveraineté, états de budget et rapports.

```
                     ┌──────────────────────────────────────────────┐
   Applications ───► │   Gateway IA (Envoy / LiteLLM / Gateway API)  │ ───► Fournisseurs LLM
                     │                 [ plan de données ]           │      (OpenAI, Mistral/Foundry,
                     └───────────────┬──────────────────────────────┘       auto-hébergé, …)
                                     │ télémétrie (aigw/OTel · Prometheus · configmap)
                                     ▼
   CRDs (déclaratif)   ┌──────────────────────────────────────────────┐
   AIGateway           │        AI Sovereign FinOps Operator           │
   AIProvider/AIModel  │             [ plan de contrôle ]              │
   AIBudgetPolicy      │  costengine · budgetengine · sovereigntyengine│
   AISovereigntyPolicy │  breakevenengine · reporting                  │
   AIBreakEvenAnalysis └───────────────┬──────────────────────────────┘
   AIFinOpsReport                      │
                                       ├─► métriques  ai_finops_*  (Prometheus → Grafana)
                                       ├─► .status des CRDs (coût, budget, constats, reco)
                                       └─► rapport Markdown/JSON (ConfigMap)
```

---

## 3. Fonctionnalités

Six moteurs purs (testés unitairement, sans dépendance K8s) pilotés par les controllers :

- **Cost engine** — calcule le coût (EUR) par requête/modèle/équipe/namespace à partir des prix par
  million de tokens déclarés sur chaque `AIProvider` ; agrège les tokens d'entrée/sortie et le % d'économies.
- **Budget engine** — suit la dépense par cible (namespace/équipe/app) sur une période, calcule le %
  d'usage et la **phase** (`ok` → `warning` → `critical` → `hardLimit`), avec actions par seuil et
  **modèle de repli**. En mode `enforce`, l'opérateur peut **rerouter réellement** vers un fallback
  **managé** moins cher au gateway, sous garde-fous mesurés.
- **Sovereignty engine** — confronte chaque appel aux règles de **résidence** (zones autorisées /
  interdites, l'UE couvrant les États membres) et de **données sensibles** (fournisseurs externes
  autorisés ou non), et remonte les **constats de violation**.
- **Break-even engine** — calcule le point mort entre **API managée** et **auto-hébergement** (coût
  mensuel comparé, économie, **payback** en mois) et émet une recommandation.
- **Reporting engine** — consolide coûts, top modèles, constats de souveraineté et recommandations dans
  un **rapport Markdown/JSON** publié en `ConfigMap`, et dans le `.status` des CRDs.
- **Recommendation engine** — produit des recommandations chiffrées et **conscientes de la souveraineté** :
  un swap cost-saving n'est **jamais** proposé vers un modèle en zone interdite, si bon marché soit-il
  (il reroute uniquement vers le modèle conforme le moins cher).
- **Enforcement engine** — transforme les constats critiques en **décisions d'enforcement** selon
  l'`enforcementMode` (`report` / `warn` / `reroute` / `block`), portées par des Events Kubernetes et la
  métrique `ai_finops_enforcement_actions`. En mode `enforce`, l'opérateur **actue réellement** dans
  **Envoy AI Gateway** : reroute vers un backend conforme quand il existe, sinon blocage par backend
  réservé absent (`aiops-blocked`). Les mutations de l'`AIGatewayRoute` sont réversibles. C'est le
  passage d'**observateur** à **plan de contrôle**.
- **Quality gate par application** — `AIQualityGate` vérifie qu'un modèle candidat ne dégrade pas une
  application avant reroute : golden dataset, preuves déterministes, télémétrie réelle source/candidat,
  seuils erreur/latence et verdict auditable `candidate-safe` / `candidate-risk` / `insufficient-data`.

Transverse :

- **Catalogue par défaut (autonomie)** — catalogue intégré des providers/modèles connus (prix de liste
  publics + zone) et `EndpointToZone(host)` : l'opérateur produit coût **et** souveraineté **dès
  l'installation**, sans écrire un seul `AIProvider`/`AIModel` (les CRs ne font qu'affiner/override). Un
  modèle inconnu génère un **AIModel stub** + une reco `data-quality`. Voir
  [`docs/features/catalog.md`](docs/features/catalog.md).
- **Plan eBPF / Shadow-AI (indépendant de la gateway)** — un plan de souveraineté qui capte le trafic
  **contournant** la gateway : **Tetragon** (eBPF) observe l'egress par pod, l'opérateur classe la
  destination par zone (`EndpointToZone`) et expose `ai_finops_shadow_ai_egress`. C'est l'angle mort que
  les collecteurs gateway ne voient pas. Voir [`docs/features/shadowengine.md`](docs/features/shadowengine.md).
- **Collecteurs de télémétrie** : `aigw` (Envoy AI Gateway / OpenTelemetry — chemin réel de production,
  lit `gen_ai_client_token_usage`), `prometheus` et `configmap` opérationnels. **Aucun repli `fake`
  silencieux** : sans source de télémétrie réelle, l'opérateur remonte une condition `NoTelemetrySource`
  explicite plutôt que d'inventer des chiffres. Le mode `fake` n'existe que sur opt-in explicite (démo).
- **Observabilité** : famille de métriques `ai_finops_*` (§7) + **dashboard Grafana** fourni
  ([`dashboards/ai-finops-overview.json`](dashboards/ai-finops-overview.json)) + `ServiceMonitor`
  optionnel (Prometheus Operator).
- **Réconciliation native** : `observedGeneration`, conditions standard, events, logs structurés ; les
  `AIModel` re-réconcilient sur changement de leur `AIProvider`.

---

## 4. Avantages

- **Déclaratif et GitOps-natif** : toute la gouvernance (budgets, souveraineté, catalogue) est en YAML
  versionné, réconcilié en continu, déployable via ArgoCD.
- **Intrusion contrôlée** : lecture seule par défaut ; l'opérateur ne mute le plan de données que
  dans des chemins d'enforcement explicitement déclarés (`AISovereigntyPolicy` ou fallback budget
  managé sur `AIBudgetPolicy`).
- **Souveraineté de premier plan** : contraintes dures de résidence et de données sensibles — absentes
  des routeurs cost/quality classiques et des gateways génériques.
- **FinOps actionnable** : attribution fine + budgets avec dégradation gracieuse + point mort
  managé/auto-hébergé, le tout exposé en métriques exploitables.
- **Préparation à l'audit** : constats horodatés et rapports reproductibles pour RGPD / AI Act.
- **100 % OSS/CNCF** : pas de dépendance propriétaire ; portable sur n'importe quel cluster conforme.
- **Sécurisé par défaut** : conteneur non-root, FS racine en lecture seule, capabilities `drop ALL`,
  RBAC au moindre privilège, `seccomp RuntimeDefault`.

---

## 5. Les CRDs (`aiops.imperium.io/v1alpha1`)

Le groupe `aiops.imperium.io/v1alpha1` expose **18 CRDs**, réconciliées par un **opérateur unique**
(un seul controller-manager, [`cmd/main.go`](cmd/main.go)) enregistrant **17 controllers**. Elles se
répartissent en deux domaines : (a) **FinOps & souveraineté** (plan de contrôle des coûts/routage) et
(b) **gouvernance confidentielle & attestation** (placement vérifiable sur nœuds SEV-SNP).

### 5.1 FinOps & souveraineté

| CRD | shortName | Rôle | Champs clés (spec) | Résultat (status) |
|---|---|---|---|---|
| **AIGateway** | `aigw` | Gateway IA observée + mode de télémétrie | `type`, `endpoint`, `telemetry.mode`, `namespaceSelector` | `governedNamespaces` |
| **AIProvider** | `aiprov` | Fournisseur (managé/auto-hébergé), pricing, conformité | `region`, `dataResidency`, `managed`, `pricing.{input,output}TokenPricePerMillion`, `compliance.{allowedForSensitiveData,allowedCountries}` | conditions |
| **AIModel** | — | Modèle catalogué, lié à un provider | `providerRef`, `modelName`, `qualityTier`, `costTier`, `contextWindow`, `sensitiveDataAllowed` | `resolvedProvider` |
| **AIBudgetPolicy** | `aibudget` | Budget par namespace/équipe/app + seuils + fallback managé optionnel | `target`, `period`, `budgetEUR`, `warning/critical/hardLimitPercent`, `actions`, `fallbackModelRef`, `enforcementMode`, `fallbackOnPhase` | `currentSpendEUR`, `usagePercent`, `phase`, `activeFallbackModel`, `fallbackActuated` |
| **AISovereigntyPolicy** | `aisov` | Règles de résidence / données sensibles / audit | `dataResidency.{allowed,forbidden}Zones`, `sensitiveData.externalProvidersAllowed`, `audit`, `enforcementMode` | `findingsCount` |
| **AIBreakEvenAnalysis** | `aibreakeven` | Point mort API managée vs auto-hébergement | `currentModelRef`, `alternativeSelfHosted`, `analysisWindowDays` | `managed/selfHostedMonthlyCostEUR`, `monthlySavingsEUR`, `paybackMonths`, `recommendation` |
| **AIFinOpsReport** | `aireport` | Rapport consolidé généré | `target`, `period`, `gatewayRef` | `totalCostEUR`, `totalInput/OutputTokens`, `topModels`, `sovereigntyFindings`, `recommendations` |
| **AIQualityGate** | `aiqgate` | Validation qualité par application avant changement de modèle | `target`, `sourceModel`, `candidateModel`, `goldenDatasetRef`, `evidenceRef`, `evaluation`, `requiredChecks`, `canary`, `rollback` | `phase`, `verdict`, `qualityScore`, `evaluationJobPhase`, `source/candidateObservation` |
| **AIRoutingPolicy** | `airpolicy` | Politique de routage inter-modèles/providers | `target`, `rules`, `enforcementMode` | conditions, décisions de routage |
| **AIRouteOverride** | `airoverride` | Override ponctuel d'une route gateway | `routeRef`, `override` | `applied`, conditions |
| **AIChangeRequest** | `aicrq` | Demande de changement gouvernée (workflow d'approbation) | `target`, `change`, `approval` | `phase`, `verdict` |

### 5.2 Gouvernance confidentielle & attestation (SEV-SNP)

Ces CRDs portent la chaîne de confiance RATS et le placement vérifiable (cœur de l'article Q1). Le
**central-verifier est l'unique writer** de `AttestationEvidence` ; le `node-attestation-agent` ne
produit que des `RawAttestationReport` non fiables.

| CRD | shortName | Rôle | Champs clés (spec) | Résultat (status) |
|---|---|---|---|---|
| **ConfidentialInferencePolicy** | `cip` | Exigence TEE/fraîcheur/runtime pour un workload sensible | `requiredTEE`, `maxEvidenceAge`, `allowedRuntimeClasses` | conditions |
| **RawAttestationReport** | `rar` | Rapport brut **non appraisé** émis par l'agent nœud | `nodeName`, `rawQuote`, `maaToken` | `submitted` |
| **AttestationEvidence** | `aevid` | Évidence **appraisée** (seul le central-verifier écrit) | `nodeName`, `teeType`, `evidenceMode`, `maaTokenHash`, `claimsDigest` | `verificationStatus`, `verifiedAt`, `expiresAt` |
| **AIPlacementDecision** | `apd` | Décision de placement signée (token Ed25519) | `podRef`, `nodeName`, `evidenceRef`, `policyRef` | `token`, `verified`, `boundAt` |
| **AIKeyReleasePolicy** | `akrp` | Conditions de libération de clé liées à l'attestation | `requiredEvidence`, `keyRef` | conditions |
| **AIRevocationPolicy** | `airvp` | Révocation d'évidence/nœud | `target`, `reason` | `revokedAt` |
| **AIEvidenceRecord** | `aier` | Journal d'audit d'évidence | `evidenceRef`, `record` | conditions |

Documentation détaillée par CRD : [`docs/crds/`](docs/crds/) · par moteur : [`docs/features/`](docs/features/).

---

## 6. Dépendances

**Exécution (runtime) :**

- **Kubernetes ≥ 1.29** (compilé contre `k8s.io/*` v0.29, `sigs.k8s.io/controller-runtime` v0.17).
- **Prometheus client** (`prometheus/client_golang`) pour l'exposition des métriques.
- *Optionnel* : **Prometheus** (collecteur de télémétrie + scraping), **Grafana** (dashboard fourni),
  **Prometheus Operator** (`ServiceMonitor`), une **gateway IA** (Envoy AI Gateway, LiteLLM, Gateway
  API…) comme plan de données.

**Build / développement :** Go 1.25 · Kubebuilder 3.14 · controller-gen v0.18.0 · kustomize v5.4.3 ·
kind 0.31 (k8s 1.35) · envtest k8s 1.31 · Helm.

**Automatisation :** `docker`, `kind`, `kubectl`, `helm` ; `git` (mode GitOps) ; `az` (chemin Azure/AKS).
Tout est CNCF/OSS.

---

## 7. Observabilité

Métriques exposées (`/metrics`, par défaut `:8080`) :

> **Convention de nommage** — ces métriques sont des **agrégats par fenêtre de reporting** (gauges), pas
> des compteurs monotones : elles n'ont donc **pas** le suffixe `_total` (qui, par convention Prometheus,
> casse `rate()`/`increase()` sur une gauge). Les compteurs cumulés côté gateway gardent `_total` dans
> leurs propres exporters.

| Métrique | Description |
|---|---|
| `ai_finops_cost_eur` | coût observé (EUR) par namespace |
| `ai_finops_input_tokens` / `ai_finops_output_tokens` | tokens entrée / sortie par namespace |
| `ai_finops_requests` | volume de requêtes par namespace |
| `ai_finops_cost_by_zone_eur` | dépense par zone de souveraineté (UE vs US…) |
| `ai_finops_budget_usage_percent` | % d'usage d'un budget |
| `ai_finops_sovereignty_findings` / `ai_finops_sovereignty_requests` | constats / requêtes à risque |
| `ai_finops_recommendations` | recommandations émises (par type/app/sévérité) |
| `ai_finops_cost_saving_eur` / `ai_finops_potential_savings_eur` | économie d'un swap / total potentiel |
| `ai_finops_enforcement_actions` | **décisions d'enforcement** (policy, namespace, app, mode, action, actuated) |
| `ai_finops_quality_score` | score qualité composite `0..100` par app, provider, modèle et dimension |
| `ai_finops_quality_gate_passed` / `ai_finops_quality_gate_failed_checks` | verdict et échecs des `AIQualityGate` par application |
| `ai_finops_breakeven_savings_eur` | économie estimée managé vs auto-hébergé |

Dashboard Grafana prêt à l'emploi : [`dashboards/ai-finops-overview.json`](dashboards/ai-finops-overview.json)
(inclut le tableau **Enforcement actions** et la dépense par zone de souveraineté).

---

## 8. Démarrage rapide

### Local (kind, sans build d'image)

```bash
make install                 # CRDs dans le cluster courant
make run                     # lance le manager depuis l'hôte (Ctrl-C pour arrêter)
kubectl apply -k config/samples/         # catalogue + policies d'exemple
kubectl get aigw,aiprov,aimodel,aibudget,aisov,aibreakeven,aireport,aiqgate
make export-report REPORT=ai-report-all NAMESPACE=default
```

### Via Helm

```bash
helm install greenops charts/ai-sovereign-finops-operator \
  --namespace greenops-system --create-namespace
# kind / image locale :
#   --set image.repository=greenops --set image.tag=dev --set image.pullPolicy=Never
```

Le chart installe CRDs, Deployment (non-root, sécurisé), RBAC, Service métriques et `ServiceMonitor`
optionnel. Valeurs : [`charts/ai-sovereign-finops-operator/values.yaml`](charts/ai-sovereign-finops-operator/values.yaml).

Guide de démo détaillé : [`docs/DEMO_KIND.md`](docs/DEMO_KIND.md).

---

## 9. Automatisations (`../automatisation/`)

Déploiement **de bout en bout, sans bricolage** — voir [`automatisation/README.md`](../automatisation/README.md).

### A. GitOps avec ArgoCD

```bash
cd ../automatisation
make up        # kind → image → ArgoCD → Applications depuis le repo GitHub/origin → wait sync
# ou:
make up-gitea  # même chaîne, avec dépôt Gitea in-cluster auto-contenu
```

`make up` enchaîne des étapes scriptées et idempotentes :

| Étape | Cible | Action |
|---|---|---|
| 1 | `cluster` | crée le cluster **kind** ([`kind/kind-config.yaml`](../automatisation/kind/kind-config.yaml)) |
| 2 | `image` | build l'image de l'opérateur et la **charge dans kind** |
| 3 | `argocd` | installe **ArgoCD** (UI sur `:30080`) |
| 4 | `apps` | crée l'`AppProject` + 2 `Application` : opérateur (chart Helm) + samples (catalogue/policies) |
| 5 | `wait` | attend que les Applications soient **Synced + Healthy** |

`make down` détruit le cluster. Manifests ArgoCD : [`automatisation/argocd/`](../automatisation/argocd/).

### B. Offline (Helm direct, sans ArgoCD)

```bash
cd ../automatisation
make local     # kind + image + Helm + samples + Shadow-AI, aucun composant GitOps requis
```

### C. Azure / AKS (déploiement cloud)

[`automatisation/azure/`](../automatisation/azure/) automatise un déploiement **discipliné en coût** sur
**AKS (Free tier)** : création du cluster, **Azure Key Vault** (RBAC + addon CSI) pour les secrets,
synchronisation des secrets vers Kubernetes (`kv-to-k8s.sh`), déploiement de la stack et d'un fournisseur
**Mistral via Azure AI Foundry** (zone de données UE). `down.sh` arrête/supprime pour ne pas payer à
l'arrêt. Cloud-agnostique par ailleurs : voir [`docs/INSTALL_AKS.md`](docs/INSTALL_AKS.md).

### D. Démonstration complète (une commande)

[`automatisation/demo/`](../automatisation/demo/) montre **toutes les fonctionnalités** sur kind, avec
**Prometheus + Grafana** déployés :

```bash
cd ../automatisation/demo && ./demo.sh up
```

Déploie l'opérateur + des CRs couvrant tous les moteurs, imprime un **tour guidé** (catalogue, coûts,
souveraineté par flux, dépassement de budget, break-even, rapport), puis ouvre le dashboard Grafana
sur `http://localhost:3000`. Détails : [`automatisation/demo/README.md`](../automatisation/demo/README.md).

### E. Envoy local

[`automatisation/envoy-local/`](../automatisation/envoy-local/) lance un **Envoy réel** en frontal du
fournisseur pour mesurer le surcoût du plan de données (overhead négligeable, 0 erreur).

---

## 10. Développement

```bash
make manifests generate     # régénère CRDs, RBAC, code deepcopy
make test                   # tests unitaires (moteurs) + envtest (controllers)
make build                  # binaire manager
make docker-build IMG=…     # image conteneur
make lint                   # golangci-lint + yamllint
```

Conventions et architecture : [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) ·
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md).

---

## 11. Statut & limites

MVP complet (Sprints 1→6) **validé de bout en bout sur kind via l'image déployée par Helm**. Le repo
compte désormais **18 CRDs** et **17 controllers** pilotés par un **opérateur unique**, plus 6 moteurs
purs, collecteurs, observabilité, reporting, chart et automatisation. S'y ajoute la brique de
gouvernance confidentielle (chaîne d'attestation SEV-SNP, scheduler attestation-aware,
central-verifier, node-attestation-agent, verify-placement) livrée en binaires de service distincts.

**Enforcement livré (slices 1, 2 et budget fallback managé)** : l'opérateur **agit** selon l'`enforcementMode`
— Events Kubernetes différenciés et métrique `ai_finops_enforcement_actions`, validés en réel sur les
violations US et les reroutes budgétaires. En mode `enforce`, le contrôle est **actué dans le plan de
données** Envoy AI Gateway : reroute vers backend conforme/fallback budgétaire avec réécriture du `model`,
ou blocage via `aiops-blocked` quand aucun fallback conforme n'existe. Le revert est automatique au retour
en `reportOnly`/`warn` ou à la suppression de la policy (finalizer). Reste : budget reroute plus fin qu'au
niveau modèle partagé, durcissement télémétrie (latence/erreurs, reset compteurs).

Détails : [`docs/ROADMAP.md`](docs/ROADMAP.md) · [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) · toute la
doc : [`docs/README.md`](docs/README.md).

---

## 12. Licence

Apache 2.0.
