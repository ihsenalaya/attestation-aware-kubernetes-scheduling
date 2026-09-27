// Package maa verifies Microsoft Azure Attestation (MAA) / Azure Guest
// Attestation tokens for AMD SEV-SNP confidential VMs.
//
// A MAA token is a JWT signed by the MAA instance. Real verification means:
//  1. parse the JWT,
//  2. fetch the issuer's JWKS (<iss>/certs) and verify the RS256 signature,
//  3. validate the SEV-SNP attestation-type claim,
//  4. validate freshness (nonce + exp/iat),
//  5. optionally bind the token to a node.
//
// HONESTY CONTRACT (Q1): this package NEVER fabricates success. If the signing
// keys cannot be fetched or the signature cannot be checked, it returns
// VerificationStatusUnavailable — it does NOT return Verified. A token can only
// be Verified when its signature is cryptographically validated.
package maa

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// Status enumerates verification outcomes. It mirrors the CRD enum.
const (
	StatusVerified    = "Verified"
	StatusFailed      = "Failed"
	StatusUnavailable = "Unavailable"
)

// Claims is the subset of MAA/guest-attestation claims we care about.
type Claims struct {
	Issuer          string                 `json:"iss"`
	IssuedAt        int64                  `json:"iat"`
	NotBefore       int64                  `json:"nbf"`
	ExpiresAt       int64                  `json:"exp"`
	Nonce           string                 `json:"nonce"`
	AttestationType string                 `json:"x-ms-attestation-type"`
	ComplianceState string                 `json:"x-ms-compliance-status"`
	IsolationTEE    map[string]interface{} `json:"x-ms-isolation-tee"`
	Raw             map[string]interface{} `json:"-"`
}

// Options controls verification strictness.
type Options struct {
	// ExpectedNonce, when set, must match the token nonce exactly.
	ExpectedNonce string
	// ExpectedTEE, when set, must match the attestation type (e.g. "sevsnpvm").
	ExpectedTEE string
	// MaxAge bounds token freshness (iat .. now). Zero disables the age check.
	MaxAge time.Duration
	// Now overrides the clock (tests).
	Now func() time.Time
	// KeyFunc overrides JWKS fetching (tests / offline). When nil, the issuer's
	// /certs endpoint is fetched over HTTPS.
	KeyFunc jwt.Keyfunc
	// HTTPClient used for JWKS fetch. Defaults to a 15s-timeout client.
	HTTPClient *http.Client
	// AllowedIssuerPrefix, when set, requires iss to start with it (e.g.
	// "https://" + provider). Guards against issuer spoofing.
	AllowedIssuerPrefix string
}

// Result is the verification outcome. Status is authoritative.
type Result struct {
	Status       string
	Reason       string
	Claims       *Claims
	ClaimsDigest string
	TokenHash    string
	IssuedAt     time.Time
	ExpiresAt    time.Time
}

func now(o Options) time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now().UTC()
}

// TokenHash returns the SHA-256 hex of a raw token (for tamper-evident logging).
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ParseMAAToken parses the JWT WITHOUT verifying the signature. Use only to peek
// at the issuer before fetching keys; never trust unverified claims.
func ParseMAAToken(token string) (*Claims, error) {
	parser := jwt.NewParser()
	raw := jwt.MapClaims{}
	if _, _, err := parser.ParseUnverified(token, raw); err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	return claimsFromMap(raw), nil
}

func claimsFromMap(m jwt.MapClaims) *Claims {
	c := &Claims{Raw: map[string]interface{}(m)}
	if v, ok := m["iss"].(string); ok {
		c.Issuer = v
	}
	if v, ok := m["nonce"].(string); ok {
		c.Nonce = v
	}
	if v, ok := m["x-ms-attestation-type"].(string); ok {
		c.AttestationType = v
	}
	if v, ok := m["x-ms-compliance-status"].(string); ok {
		c.ComplianceState = v
	}
	if v, ok := m["x-ms-isolation-tee"].(map[string]interface{}); ok {
		c.IsolationTEE = v
	}
	if c.Nonce == "" {
		c.Nonce = extractNonceFromRuntime(m["x-ms-runtime"])
	}
	if c.Nonce == "" && c.IsolationTEE != nil {
		c.Nonce = extractNonceFromRuntime(c.IsolationTEE["x-ms-runtime"])
	}
	if c.ComplianceState == "" && c.IsolationTEE != nil {
		if v, ok := c.IsolationTEE["x-ms-compliance-status"].(string); ok {
			c.ComplianceState = v
		}
	}
	c.IssuedAt = asInt64(m["iat"])
	c.NotBefore = asInt64(m["nbf"])
	c.ExpiresAt = asInt64(m["exp"])
	return c
}

func extractNonceFromRuntime(v interface{}) string {
	runtime, ok := v.(map[string]interface{})
	if !ok {
		return ""
	}
	payload := runtime["client-payload"]
	switch p := payload.(type) {
	case map[string]interface{}:
		for _, key := range []string{"nonce", "Nonce"} {
			if raw, ok := p[key].(string); ok {
				return decodeRuntimePayloadValue(raw)
			}
		}
	case string:
		decoded := decodeRuntimePayloadValue(p)
		var m map[string]interface{}
		if json.Unmarshal([]byte(decoded), &m) == nil {
			for _, key := range []string{"nonce", "Nonce"} {
				if raw, ok := m[key].(string); ok {
					return decodeRuntimePayloadValue(raw)
				}
			}
		}
		return decoded
	}
	return ""
}

func decodeRuntimePayloadValue(s string) string {
	if s == "" {
		return ""
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) > 0 {
		return string(b)
	}
	return s
}

func asInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

// ExtractClaims returns the claims from a token (unverified parse).
func ExtractClaims(token string) (*Claims, error) { return ParseMAAToken(token) }

// ComputeClaimsDigest returns a deterministic SHA-256 over the canonical JSON of
// the security-relevant claims. Used to bind evidence to exact claims.
func ComputeClaimsDigest(c *Claims) (string, error) {
	if c == nil {
		return "", fmt.Errorf("nil claims")
	}
	canonical := map[string]interface{}{
		"iss":                    c.Issuer,
		"iat":                    c.IssuedAt,
		"exp":                    c.ExpiresAt,
		"nonce":                  c.Nonce,
		"x-ms-attestation-type":  c.AttestationType,
		"x-ms-compliance-status": c.ComplianceState,
		"x-ms-isolation-tee":     c.IsolationTEE,
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateFreshness checks nonce match and token age.
func ValidateFreshness(c *Claims, expectedNonce string, maxAge time.Duration, clock time.Time) error {
	if expectedNonce != "" && c.Nonce != expectedNonce {
		return fmt.Errorf("nonce mismatch")
	}
	if c.ExpiresAt > 0 && clock.Unix() > c.ExpiresAt {
		return fmt.Errorf("token expired")
	}
	if c.NotBefore > 0 && clock.Unix()+1 < c.NotBefore {
		return fmt.Errorf("token not yet valid")
	}
	if maxAge > 0 && c.IssuedAt > 0 {
		age := clock.Sub(time.Unix(c.IssuedAt, 0))
		if age > maxAge {
			return fmt.Errorf("token age %s exceeds max %s", age.Round(time.Second), maxAge)
		}
	}
	return nil
}

// ValidateTEEClaims checks the SEV-SNP attestation type. It inspects both the
// top-level claim and the isolation-tee sub-claim (guest attestation nests it).
func ValidateTEEClaims(c *Claims, expectedTEE string) error {
	if expectedTEE == "" {
		return nil
	}
	want := strings.ToLower(expectedTEE)
	if strings.ToLower(c.AttestationType) == want {
		return nil
	}
	if c.IsolationTEE != nil {
		if v, ok := c.IsolationTEE["x-ms-attestation-type"].(string); ok && strings.ToLower(v) == want {
			return nil
		}
	}
	return fmt.Errorf("attestation-type %q does not match expected %q", c.AttestationType, expectedTEE)
}

// ValidateNodeBinding checks that the token nonce embeds the node identity when a
// binding is expected. Guest attestation does not natively carry the node name,
// so we bind via the agent-chosen nonce (nonce = H(nodeUID|freshness)).
func ValidateNodeBinding(c *Claims, expectedNonce string) error {
	if expectedNonce == "" {
		return nil
	}
	if c.Nonce != expectedNonce {
		return fmt.Errorf("node-binding nonce mismatch")
	}
	return nil
}

// VerifyMAAToken performs full verification. It returns a Result whose Status is
// Verified only when the signature was cryptographically validated AND all
// requested claim checks pass. If keys are unreachable, Status is Unavailable.
func VerifyMAAToken(token string, opts Options) Result {
	res := Result{TokenHash: TokenHash(token)}
	clock := now(opts)

	// Peek at issuer to choose the JWKS source (still unverified here).
	peek, err := ParseMAAToken(token)
	if err != nil {
		res.Status = StatusFailed
		res.Reason = err.Error()
		return res
	}
	if opts.AllowedIssuerPrefix != "" && !strings.HasPrefix(peek.Issuer, opts.AllowedIssuerPrefix) {
		res.Status = StatusFailed
		res.Reason = fmt.Sprintf("issuer %q not allowed", peek.Issuer)
		return res
	}

	keyFunc := opts.KeyFunc
	if keyFunc == nil {
		jwks, ferr := fetchJWKS(peek.Issuer, opts.HTTPClient, clock)
		if ferr != nil {
			// Cannot verify signature → be honest, do NOT claim Verified.
			res.Status = StatusUnavailable
			res.Reason = fmt.Sprintf("cannot fetch signing keys: %v", ferr)
			res.Claims = peek
			return res
		}
		keyFunc = jwks
	}

	parsed, perr := jwt.Parse(token, keyFunc, jwt.WithValidMethods([]string{"RS256"}))
	if perr != nil || !parsed.Valid {
		res.Status = StatusFailed
		if perr != nil {
			res.Reason = fmt.Sprintf("signature verification failed: %v", perr)
		} else {
			res.Reason = "token invalid after signature check"
		}
		return res
	}

	mc, _ := parsed.Claims.(jwt.MapClaims)
	c := claimsFromMap(mc)
	res.Claims = c

	if err := ValidateFreshness(c, opts.ExpectedNonce, opts.MaxAge, clock); err != nil {
		res.Status = StatusFailed
		res.Reason = err.Error()
		return res
	}
	if err := ValidateTEEClaims(c, opts.ExpectedTEE); err != nil {
		res.Status = StatusFailed
		res.Reason = err.Error()
		return res
	}

	digest, derr := ComputeClaimsDigest(c)
	if derr != nil {
		res.Status = StatusFailed
		res.Reason = derr.Error()
		return res
	}
	res.ClaimsDigest = digest
	if c.IssuedAt > 0 {
		res.IssuedAt = time.Unix(c.IssuedAt, 0).UTC()
	}
	if c.ExpiresAt > 0 {
		res.ExpiresAt = time.Unix(c.ExpiresAt, 0).UTC()
	}
	res.Status = StatusVerified
	res.Reason = "signature and claims verified"
	return res
}

// ─── JWKS handling ──────────────────────────────────────────────────────────

type jwk struct {
	Kid string   `json:"kid"`
	Kty string   `json:"kty"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

type jwksDoc struct {
	Keys []jwk `json:"keys"`
}

// fetchJWKS downloads the issuer's signing keys and returns a jwt.Keyfunc that
// selects the key by "kid".
func fetchJWKS(issuer string, client *http.Client, _ time.Time) (jwt.Keyfunc, error) {
	if issuer == "" {
		return nil, fmt.Errorf("empty issuer")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	url := strings.TrimRight(issuer, "/") + "/certs"
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks endpoint returned %d", resp.StatusCode)
	}
	var doc jwksDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		pub, err := rsaKeyFromJWK(k)
		if err == nil && pub != nil {
			keys[k.Kid] = pub
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no usable RSA keys in JWKS")
	}
	return func(t *jwt.Token) (interface{}, error) {
		kid, _ := t.Header["kid"].(string)
		if pub, ok := keys[kid]; ok {
			return pub, nil
		}
		// Fall back to the only key if kid is absent.
		if kid == "" && len(keys) == 1 {
			for _, pub := range keys {
				return pub, nil
			}
		}
		return nil, fmt.Errorf("no signing key for kid %q", kid)
	}, nil
}

func rsaKeyFromJWK(k jwk) (*rsa.PublicKey, error) {
	// Prefer x5c (certificate chain) when present.
	if len(k.X5c) > 0 {
		der, err := base64.StdEncoding.DecodeString(k.X5c[0])
		if err != nil {
			return nil, err
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		if pub, ok := cert.PublicKey.(*rsa.PublicKey); ok {
			return pub, nil
		}
		return nil, fmt.Errorf("x5c is not RSA")
	}
	if k.Kty != "RSA" || k.N == "" || k.E == "" {
		return nil, fmt.Errorf("unsupported jwk")
	}
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, err
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
}
