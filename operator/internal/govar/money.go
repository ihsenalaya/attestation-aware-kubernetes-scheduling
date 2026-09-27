package govar

import (
	"errors"
	"math/big"

	"k8s.io/apimachinery/pkg/api/resource"
)

const microsPerCurrencyUnit int64 = 1_000_000

func quantityToMicros(q resource.Quantity) (MoneyMicros, error) {
	rat, ok := new(big.Rat).SetString(q.AsDec().String())
	if !ok || rat.Sign() < 0 {
		return 0, errors.New("money quantity must be a non-negative decimal")
	}
	rat.Mul(rat, big.NewRat(microsPerCurrencyUnit, 1))
	quotient := new(big.Int).Quo(rat.Num(), rat.Denom())
	if !quotient.IsInt64() {
		return 0, errors.New("money quantity exceeds int64 micro-unit range")
	}
	return MoneyMicros(quotient.Int64()), nil
}

func quantityToExactMicros(q resource.Quantity) (MoneyMicros, error) {
	rat, ok := new(big.Rat).SetString(q.AsDec().String())
	if !ok || rat.Sign() < 0 {
		return 0, errors.New("money quantity must be a non-negative decimal")
	}
	rat.Mul(rat, big.NewRat(microsPerCurrencyUnit, 1))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(rat.Num(), rat.Denom(), remainder)
	if remainder.Sign() != 0 {
		return 0, errors.New("money quantity is not exactly representable in integer micro-units")
	}
	if !quotient.IsInt64() {
		return 0, errors.New("money quantity exceeds int64 micro-unit range")
	}
	return MoneyMicros(quotient.Int64()), nil
}

func expectedCostMicros(c Candidate, inputTokens, maxOutputTokens int64) (MoneyMicros, error) {
	return costFromPriceMicros(c.InputPriceMicrosPerMillion, c.OutputPriceMicrosPerMillion, inputTokens, maxOutputTokens)
}

func costFromPriceMicros(inPrice, outPrice, inputTokens, maxOutputTokens int64) (MoneyMicros, error) {
	if inputTokens < 0 || maxOutputTokens < 0 {
		return 0, errors.New("token counts cannot be negative")
	}
	total := new(big.Int).Mul(big.NewInt(inputTokens), big.NewInt(inPrice))
	total.Add(total, new(big.Int).Mul(big.NewInt(maxOutputTokens), big.NewInt(outPrice)))
	divisor := big.NewInt(1_000_000)
	q, r := new(big.Int).QuoRem(total, divisor, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		return 0, errors.New("reservation exceeds int64 micro-unit range")
	}
	return MoneyMicros(q.Int64()), nil
}
