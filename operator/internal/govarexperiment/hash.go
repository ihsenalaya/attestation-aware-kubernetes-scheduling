package govarexperiment

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
)

func SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DomainHash length-prefixes the domain and every part. It is used for
// experiment identities and not as a substitute for a cryptographic signature.
func DomainHash(domain string, parts ...[]byte) string {
	h := sha256.New()
	var length [8]byte
	write := func(value []byte) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	write([]byte(domain))
	for _, part := range parts {
		write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RandomUint64 is the sole deterministic-randomness primitive. Its domain is
// mandatory so adding another random use cannot perturb an existing stream.
func RandomUint64(domain string, seed uint64, parts ...[]byte) uint64 {
	var encodedSeed [8]byte
	binary.BigEndian.PutUint64(encodedSeed[:], seed)
	inputs := append([][]byte{encodedSeed[:]}, parts...)
	digest, _ := hex.DecodeString(DomainHash("govar-experiment-random-v1/"+domain, inputs...))
	return binary.BigEndian.Uint64(digest[:8])
}

func checkedAdd(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, errors.New("integer-micro sum overflow")
	}
	return a + b, nil
}

func ceilingProduct(price, quantity int64) (int64, error) {
	if price < 0 || quantity < 0 {
		return 0, errors.New("invalid exact monetary product")
	}
	numerator := new(big.Int).Mul(big.NewInt(price), big.NewInt(quantity))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, big.NewInt(1_000_000), remainder)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, errors.New("integer-micro product overflow")
	}
	return quotient.Int64(), nil
}

func ratePPB(numerator, denominator int64) (int64, error) {
	if numerator < 0 || denominator < 0 || numerator > denominator {
		return 0, errors.New("invalid exact rate")
	}
	if denominator == 0 {
		return 0, nil
	}
	n := new(big.Int).Mul(big.NewInt(numerator), big.NewInt(1_000_000_000))
	n.Quo(n, big.NewInt(denominator))
	if !n.IsInt64() {
		return 0, errors.New("rate overflow")
	}
	return n.Int64(), nil
}

func utilizationPPB(exposure, budget int64) (int64, error) {
	if exposure < 0 || budget < 0 {
		return 0, errors.New("negative exposure or budget")
	}
	if budget == 0 {
		return 0, nil
	}
	n := new(big.Int).Mul(big.NewInt(exposure), big.NewInt(1_000_000_000))
	n.Quo(n, big.NewInt(budget))
	if !n.IsInt64() {
		return 0, errors.New("utilization overflow")
	}
	return n.Int64(), nil
}
