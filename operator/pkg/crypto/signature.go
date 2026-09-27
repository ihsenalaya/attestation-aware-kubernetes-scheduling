package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// SHA256Hex returns the lowercase SHA-256 digest of data.
func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// HMACSHA256Hex signs data with a shared secret and returns the lowercase hex digest.
func HMACSHA256Hex(secret, data []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyHMACSHA256Hex reports whether signature matches the HMAC of data.
func VerifyHMACSHA256Hex(secret, data []byte, signature string) bool {
	expected := HMACSHA256Hex(secret, data)
	return hmac.Equal([]byte(expected), []byte(signature))
}
