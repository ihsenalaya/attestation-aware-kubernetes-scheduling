package crypto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// CanonicalJSON marshals a value into a deterministic JSON representation with
// recursively sorted object keys so hashes remain stable across map iteration.
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical json input: %w", err)
	}

	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("unmarshal canonical json input: %w", err)
	}

	var buf bytes.Buffer
	if err := writeCanonicalJSON(&buf, decoded); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// CanonicalSHA256Hex returns the SHA-256 digest of CanonicalJSON(v).
func CanonicalSHA256Hex(v any) (string, error) {
	raw, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	return SHA256Hex(raw), nil
}

func writeCanonicalJSON(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if t {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case float64, string:
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("marshal canonical scalar: %w", err)
		}
		buf.Write(raw)
	case []any:
		buf.WriteByte('[')
		for i, elem := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalJSON(buf, elem); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			keyJSON, err := json.Marshal(k)
			if err != nil {
				return fmt.Errorf("marshal canonical key: %w", err)
			}
			buf.Write(keyJSON)
			buf.WriteByte(':')
			if err := writeCanonicalJSON(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("marshal canonical fallback: %w", err)
		}
		buf.Write(raw)
	}
	return nil
}
