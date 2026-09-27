package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRequireEOFRejectsASecondJSONValue(t *testing.T) {
	decoder := json.NewDecoder(bytes.NewBufferString(`{"profiles":[]} {"profiles":[]}`))
	var first any
	if err := decoder.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := requireEOF(decoder); err == nil {
		t.Fatal("prospective evidence/config parser accepted a second JSON value")
	}
}

func TestRequireEOFAcceptsTrailingWhitespaceOnly(t *testing.T) {
	decoder := json.NewDecoder(bytes.NewBufferString("{\"profiles\":[]} \n\t"))
	var first any
	if err := decoder.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := requireEOF(decoder); err != nil {
		t.Fatalf("trailing whitespace was rejected: %v", err)
	}
}
