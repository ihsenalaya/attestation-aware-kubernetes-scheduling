package maa

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// testSigner builds a MAA-like RS256 token and a matching KeyFunc so tests
// exercise the REAL signature-verification path (no fake success).
func testSigner(t *testing.T, claims jwt.MapClaims) (string, jwt.Keyfunc) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "test-kid"
	signed, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	kf := func(_ *jwt.Token) (interface{}, error) { return &key.PublicKey, nil }
	return signed, kf
}

func baseClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss":                    "https://prov.eus2.attest.azure.net",
		"iat":                    now.Add(-30 * time.Second).Unix(),
		"nbf":                    now.Add(-30 * time.Second).Unix(),
		"exp":                    now.Add(5 * time.Minute).Unix(),
		"nonce":                  "nonce-abc",
		"x-ms-attestation-type":  "sevsnpvm",
		"x-ms-compliance-status": "azure-compliant-cvm",
	}
}

func TestVerifyValidToken(t *testing.T) {
	now := time.Now().UTC()
	tokenStr, kf := testSigner(t, baseClaims(now))
	res := VerifyMAAToken(tokenStr, Options{
		KeyFunc:       kf,
		ExpectedNonce: "nonce-abc",
		ExpectedTEE:   "sevsnpvm",
		MaxAge:        5 * time.Minute,
		Now:           func() time.Time { return now },
	})
	if res.Status != StatusVerified {
		t.Fatalf("status = %s (%s), want Verified", res.Status, res.Reason)
	}
	if res.ClaimsDigest == "" || res.TokenHash == "" {
		t.Fatal("digest/hash must be populated on success")
	}
}

func TestMalformedTokenFails(t *testing.T) {
	res := VerifyMAAToken("not-a-jwt", Options{KeyFunc: func(_ *jwt.Token) (interface{}, error) { return nil, nil }})
	if res.Status != StatusFailed {
		t.Fatalf("status = %s, want Failed", res.Status)
	}
}

func TestExpiredTokenFails(t *testing.T) {
	now := time.Now().UTC()
	c := baseClaims(now)
	c["exp"] = now.Add(-time.Minute).Unix()
	tokenStr, kf := testSigner(t, c)
	res := VerifyMAAToken(tokenStr, Options{KeyFunc: kf, Now: func() time.Time { return now }})
	if res.Status != StatusFailed {
		t.Fatalf("status = %s (%s), want Failed", res.Status, res.Reason)
	}
}

func TestWrongNonceFails(t *testing.T) {
	now := time.Now().UTC()
	tokenStr, kf := testSigner(t, baseClaims(now))
	res := VerifyMAAToken(tokenStr, Options{KeyFunc: kf, ExpectedNonce: "different", Now: func() time.Time { return now }})
	if res.Status != StatusFailed {
		t.Fatalf("status = %s, want Failed", res.Status)
	}
}

func TestWrongTEEFails(t *testing.T) {
	now := time.Now().UTC()
	c := baseClaims(now)
	c["x-ms-attestation-type"] = "tdxvm"
	tokenStr, kf := testSigner(t, c)
	res := VerifyMAAToken(tokenStr, Options{KeyFunc: kf, ExpectedTEE: "sevsnpvm", Now: func() time.Time { return now }})
	if res.Status != StatusFailed {
		t.Fatalf("status = %s, want Failed", res.Status)
	}
}

func TestBadSignatureFails(t *testing.T) {
	now := time.Now().UTC()
	tokenStr, _ := testSigner(t, baseClaims(now))
	// Verify with a DIFFERENT key -> signature must fail.
	otherKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	res := VerifyMAAToken(tokenStr, Options{
		KeyFunc: func(_ *jwt.Token) (interface{}, error) { return &otherKey.PublicKey, nil },
		Now:     func() time.Time { return now },
	})
	if res.Status != StatusFailed {
		t.Fatalf("status = %s, want Failed (bad signature)", res.Status)
	}
}

func TestUnavailableWhenKeysUnreachable(t *testing.T) {
	now := time.Now().UTC()
	// A well-formed token but an issuer whose /certs cannot be fetched.
	c := baseClaims(now)
	c["iss"] = "https://unreachable.invalid.example"
	tokenStr, _ := testSigner(t, c)
	res := VerifyMAAToken(tokenStr, Options{Now: func() time.Time { return now }})
	if res.Status != StatusUnavailable {
		t.Fatalf("status = %s (%s), want Unavailable (never fake Verified)", res.Status, res.Reason)
	}
}

func TestIssuerPrefixEnforced(t *testing.T) {
	now := time.Now().UTC()
	tokenStr, kf := testSigner(t, baseClaims(now))
	res := VerifyMAAToken(tokenStr, Options{
		KeyFunc:             kf,
		AllowedIssuerPrefix: "https://trusted.only",
		Now:                 func() time.Time { return now },
	})
	if res.Status != StatusFailed {
		t.Fatalf("status = %s, want Failed (issuer not allowed)", res.Status)
	}
}

func TestIsolationTEENestedClaim(t *testing.T) {
	now := time.Now().UTC()
	c := baseClaims(now)
	c["x-ms-attestation-type"] = "" // top-level empty
	c["x-ms-isolation-tee"] = map[string]interface{}{"x-ms-attestation-type": "sevsnpvm"}
	tokenStr, kf := testSigner(t, c)
	res := VerifyMAAToken(tokenStr, Options{KeyFunc: kf, ExpectedTEE: "sevsnpvm", Now: func() time.Time { return now }})
	if res.Status != StatusVerified {
		t.Fatalf("status = %s (%s), want Verified via nested isolation-tee", res.Status, res.Reason)
	}
}

func TestAzureGuestRuntimePayloadNonce(t *testing.T) {
	now := time.Now().UTC()
	c := baseClaims(now)
	delete(c, "nonce")
	c["x-ms-attestation-type"] = "azurevm"
	c["x-ms-runtime"] = map[string]interface{}{
		"client-payload": map[string]interface{}{
			"nonce": base64.StdEncoding.EncodeToString([]byte("nonce-from-runtime")),
			"Nonce": base64.StdEncoding.EncodeToString([]byte("nonce-from-runtime")),
		},
	}
	c["x-ms-isolation-tee"] = map[string]interface{}{
		"x-ms-attestation-type":  "sevsnpvm",
		"x-ms-compliance-status": "azure-compliant-cvm",
	}
	tokenStr, kf := testSigner(t, c)
	res := VerifyMAAToken(tokenStr, Options{
		KeyFunc:       kf,
		ExpectedNonce: "nonce-from-runtime",
		ExpectedTEE:   "sevsnpvm",
		MaxAge:        5 * time.Minute,
		Now:           func() time.Time { return now },
	})
	if res.Status != StatusVerified {
		t.Fatalf("status = %s (%s), want Verified via x-ms-runtime client payload nonce", res.Status, res.Reason)
	}
	if got := res.Claims.Nonce; got != "nonce-from-runtime" {
		t.Fatalf("nonce = %q, want runtime nonce", got)
	}
}

func TestClaimsDigestDeterministic(t *testing.T) {
	now := time.Now().UTC()
	c := baseClaims(now)
	claims := claimsFromMap(c)
	d1, err := ComputeClaimsDigest(claims)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	d2, _ := ComputeClaimsDigest(claims)
	if d1 != d2 || d1 == "" {
		t.Fatalf("digest not deterministic: %q vs %q", d1, d2)
	}
}
