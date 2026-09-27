package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	platformcrypto "github.com/imperium/ai-sovereign-finops-operator/pkg/crypto"
	"github.com/imperium/ai-sovereign-finops-operator/pkg/token"
)

func main() {
	var (
		tokenRaw     = flag.String("token", "", "JSON-encoded placement token")
		tokenFile    = flag.String("token-file", "", "path to JSON-encoded placement token")
		pubKeyHex    = flag.String("pubkey-hex", "", "hex-encoded Ed25519 public key")
		pubKeyFile   = flag.String("pubkey-file", "", "file containing the hex-encoded Ed25519 public key")
		podUID       = flag.String("pod-uid", "", "expected pod UID")
		podSpecHash  = flag.String("pod-spec-hash", "", "expected canonical pod spec hash")
		nodeIdentity = flag.String("node", "", "expected node identity")
		evidenceHash = flag.String("evidence-hash", "", "expected evidence hash")
		policyHash   = flag.String("policy-hash", "", "expected policy hash")
	)
	flag.Parse()

	rawToken, err := readValue(*tokenRaw, *tokenFile)
	if err != nil {
		fail(err)
	}
	if strings.TrimSpace(rawToken) == "" {
		fail(fmt.Errorf("token or token-file is required"))
	}

	rawPubKey, err := readValue(*pubKeyHex, *pubKeyFile)
	if err != nil {
		fail(err)
	}
	if strings.TrimSpace(rawPubKey) == "" {
		fail(fmt.Errorf("pubkey-hex or pubkey-file is required"))
	}

	pub, err := platformcrypto.PubKeyFromHex(strings.TrimSpace(rawPubKey))
	if err != nil {
		fail(fmt.Errorf("parse public key: %w", err))
	}

	tok, err := token.Decode(strings.TrimSpace(rawToken))
	if err != nil {
		fail(fmt.Errorf("decode token: %w", err))
	}

	if *podUID == "" {
		if err := token.Verify(pub, tok); err != nil {
			fail(err)
		}
		fmt.Fprintln(os.Stdout, "PASS")
		return
	}

	if err := token.VerifyForPod(pub, tok, *podUID, *podSpecHash, *nodeIdentity, *evidenceHash, *policyHash); err != nil {
		fail(err)
	}
	fmt.Fprintln(os.Stdout, "PASS")
}

func readValue(raw, path string) (string, error) {
	if strings.TrimSpace(raw) != "" {
		return raw, nil
	}
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return string(data), nil
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
	os.Exit(1)
}
