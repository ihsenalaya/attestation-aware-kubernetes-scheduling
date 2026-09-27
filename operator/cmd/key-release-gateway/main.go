// key-release-gateway is the conditional key-release HTTP service.
//
// Endpoints:
//
//	POST /v1/key-release  — evaluate and respond to a key-release request
//	GET  /healthz         — liveness probe
//	GET  /readyz          — readiness probe
//	GET  /metrics         — Prometheus metrics
//
// The gateway verifies:
//   - valid placement token (Ed25519 signature + expiry)
//   - evidence freshness (MaxEvidenceAgeSeconds)
//   - evidence not revoked
//   - pod UID binding
//   - model digest binding
//   - image digest binding
//   - policy hash binding
//   - active AIRevocationPolicy
//   - audit requirement if requested by policy
//
// Backends (key material store):
//   - memory  : kind/dev — stores keys in-process
//   - secret  : reads from Kubernetes Secrets (dev)
//   - planned : vault, trustee, azure-key-vault (interface documented)
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"crypto/ed25519"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/imperium/ai-sovereign-finops-operator/internal/keyrelease"
)

var (
	requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ai_key_release_requests_total",
		Help: "Total key release requests by outcome",
	}, []string{"outcome"})

	latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ai_key_release_latency_seconds",
		Help:    "Key release evaluation latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"})
)

func init() {
	prometheus.MustRegister(requestsTotal, latency)
}

// --- HTTP request / response types ---

type releaseRequest struct {
	Namespace      string `json:"namespace"`
	PodName        string `json:"podName"`
	PodUID         string `json:"podUID"`
	KeyID          string `json:"keyID"`
	ModelDigest    string `json:"modelDigest,omitempty"`
	ImageDigest    string `json:"imageDigest,omitempty"`
	PolicyHash     string `json:"policyHash,omitempty"`
	EvidenceHash   string `json:"evidenceHash,omitempty"`
	PlacementToken string `json:"placementToken,omitempty"`

	// Evidence metadata provided by the caller (in a real cluster these would
	// be fetched from AttestationEvidence CRDs via the platform-api)
	EvidenceVerified   bool      `json:"evidenceVerified"`
	EvidenceRevoked    bool      `json:"evidenceRevoked"`
	EvidenceLastSeen   time.Time `json:"evidenceLastSeen,omitempty"`
	MaxEvidenceAgeSecs int32     `json:"maxEvidenceAgeSecs,omitempty"`

	PolicyRequired      bool  `json:"policyRequired"`
	PolicyTTLSeconds    int32 `json:"policyTTLSeconds,omitempty"`
	PolicyAuditRequired bool  `json:"policyAuditRequired,omitempty"`
	RevocationActive    bool  `json:"revocationActive,omitempty"`
}

type releaseResponse struct {
	Allowed        bool                  `json:"allowed"`
	Reason         keyrelease.DenyReason `json:"reason"`
	KeyMaterialRef string                `json:"keyMaterialRef,omitempty"`
	TTLSeconds     int32                 `json:"ttlSeconds,omitempty"`
	EvidenceRecord string                `json:"evidenceRecord,omitempty"`
}

// --- in-memory key store (kind/dev backend) ---

type memoryKeyStore struct {
	mu   sync.RWMutex
	keys map[string]string
}

func newMemoryKeyStore() *memoryKeyStore {
	return &memoryKeyStore{keys: map[string]string{}}
}

func (s *memoryKeyStore) Set(keyID, material string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[keyID] = material
}

func (s *memoryKeyStore) Get(keyID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.keys[keyID]
	return v, ok
}

// --- Gateway ---

type gateway struct {
	store    *memoryKeyStore
	tokenPub ed25519.PublicKey
}

func (g *gateway) handleKeyRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()

	var req releaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
		return
	}

	if req.PodUID == "" || req.KeyID == "" {
		http.Error(w, "podUID and keyID are required", http.StatusBadRequest)
		return
	}

	kr := keyrelease.Request{
		Namespace:           req.Namespace,
		PodName:             req.PodName,
		PodUID:              req.PodUID,
		KeyID:               req.KeyID,
		ModelDigest:         req.ModelDigest,
		ImageDigest:         req.ImageDigest,
		PolicyHash:          req.PolicyHash,
		EvidenceHash:        req.EvidenceHash,
		PlacementToken:      req.PlacementToken,
		EvidenceVerified:    req.EvidenceVerified,
		EvidenceRevoked:     req.EvidenceRevoked,
		EvidenceLastSeen:    req.EvidenceLastSeen,
		MaxEvidenceAgeSecs:  req.MaxEvidenceAgeSecs,
		PolicyRequired:      req.PolicyRequired,
		PolicyTTLSeconds:    req.PolicyTTLSeconds,
		PolicyAuditRequired: req.PolicyAuditRequired,
		RevocationActive:    req.RevocationActive,
		TokenPublicKey:      g.tokenPub,
	}

	decision := keyrelease.Evaluate(kr)

	outcome := "denied"
	if decision.Allowed {
		outcome = "allowed"
	}
	requestsTotal.WithLabelValues(outcome).Inc()
	latency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())

	// Attach key material reference for allowed requests
	var keyRef string
	if decision.Allowed {
		if mat, ok := g.store.Get(req.KeyID); ok {
			keyRef = fmt.Sprintf("ref:memory:%s", mat[:min(8, len(mat))])
		} else {
			keyRef = fmt.Sprintf("key://%s", req.KeyID)
		}
	}

	resp := releaseResponse{
		Allowed:        decision.Allowed,
		Reason:         decision.Reason,
		KeyMaterialRef: keyRef,
		TTLSeconds:     decision.TTLSeconds,
	}

	w.Header().Set("Content-Type", "application/json")
	if !decision.Allowed {
		w.WriteHeader(http.StatusForbidden)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	addr := os.Getenv("KEY_RELEASE_ADDR")
	if addr == "" {
		addr = ":8082"
	}

	// Load token public key for verification (hex-encoded Ed25519 public key)
	var tokenPub ed25519.PublicKey
	if pubHex := os.Getenv("TOKEN_PUBLIC_KEY_HEX"); pubHex != "" {
		b, err := hex.DecodeString(pubHex)
		if err == nil && len(b) == ed25519.PublicKeySize {
			tokenPub = ed25519.PublicKey(b)
			log.Printf("key-release-gateway: loaded token public key from TOKEN_PUBLIC_KEY_HEX")
		} else {
			log.Printf("key-release-gateway: WARNING — TOKEN_PUBLIC_KEY_HEX invalid, token signatures will NOT be verified")
		}
	} else {
		log.Printf("key-release-gateway: WARNING — TOKEN_PUBLIC_KEY_HEX not set, token signatures will NOT be verified")
	}

	store := newMemoryKeyStore()
	// Seed with a demo key for kind testing
	store.Set("demo-key-1", "SIMULATED-KEY-MATERIAL-DO-NOT-USE-IN-PRODUCTION")

	gw := &gateway{store: store, tokenPub: tokenPub}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/key-release", gw.handleKeyRelease)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	log.Printf("key-release-gateway listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
