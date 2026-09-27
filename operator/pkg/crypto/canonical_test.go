package crypto

import "testing"

func TestCanonicalSHA256HexDeterministicForMapOrder(t *testing.T) {
	a := map[string]any{
		"b": map[string]any{"y": 2, "x": 1},
		"a": []any{
			map[string]any{"delta": "4", "gamma": "3"},
		},
	}
	b := map[string]any{
		"a": []any{
			map[string]any{"gamma": "3", "delta": "4"},
		},
		"b": map[string]any{"x": 1, "y": 2},
	}

	hashA, err := CanonicalSHA256Hex(a)
	if err != nil {
		t.Fatalf("CanonicalSHA256Hex(a): %v", err)
	}
	hashB, err := CanonicalSHA256Hex(b)
	if err != nil {
		t.Fatalf("CanonicalSHA256Hex(b): %v", err)
	}
	if hashA != hashB {
		t.Fatalf("canonical hashes differ: %s vs %s", hashA, hashB)
	}
}
