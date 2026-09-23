package dataflow

import (
	"bytes"
	"testing"

	"github.com/huginnlabs-dev/dataflow-go/encoder"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	salt, err := encoder.SaltFromHex("")
	if err != nil {
		t.Fatal(err)
	}
	key, err := encoder.DeriveKey("hunter2", salt)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(`{"user":"ada@example.com"}`)
	ct, iv, err := encoder.Encrypt(key, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(ct, plaintext) {
		t.Fatal("ciphertext equals plaintext")
	}
	got, err := encoder.Decrypt(key, ct, iv)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("roundtrip mismatch: %q", got)
	}
}

func TestDecryptWithWrongKeyFails(t *testing.T) {
	salt, _ := encoder.SaltFromHex("")
	keyA, _ := encoder.DeriveKey("key-a", salt)
	keyB, _ := encoder.DeriveKey("key-b", salt)
	ct, iv, _ := encoder.Encrypt(keyA, []byte("secret"))
	if _, err := encoder.Decrypt(keyB, ct, iv); err == nil {
		t.Fatal("expected failure decrypting with a foreign key")
	}
}

func TestDeriveKeyIsDeterministic(t *testing.T) {
	salt, _ := encoder.SaltFromHex("")
	a, _ := encoder.DeriveKey("same", salt)
	b, _ := encoder.DeriveKey("same", salt)
	if !bytes.Equal(a, b) {
		t.Fatal("PBKDF2 derivation is not deterministic")
	}
	if len(a) != encoder.KeyLen {
		t.Fatalf("key length = %d, want %d", len(a), encoder.KeyLen)
	}
}

func TestPackageOfFuncName(t *testing.T) {
	cases := map[string]string{
		"github.com/acme/shop/internal/users.Service.GetUser": "github.com/acme/shop/internal/users",
		"main.main":                                          "main",
		"net/http.(*Client).Do":                              "net/http",
		"":                                                   "",
		"bareFunc":                                           "",
	}
	for in, want := range cases {
		if got := packageOfFuncName(in); got != want {
			t.Errorf("packageOfFuncName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseEndpoint(t *testing.T) {
	host, tls := parseEndpoint("https://api.example.com", false)
	if host != "api.example.com:443" || !tls {
		t.Errorf("https endpoint = %q, %v", host, tls)
	}
	host, tls = parseEndpoint("ingest:9090", false)
	if host != "ingest:9090" || tls {
		t.Errorf("host:port endpoint = %q, %v", host, tls)
	}
	host, tls = parseEndpoint("http://localhost:9090", false)
	if host != "localhost:9090" || tls {
		t.Errorf("http endpoint = %q, %v", host, tls)
	}
	host, tls = parseEndpoint("https://api.example.com", true)
	if tls {
		t.Error("Insecure must disable TLS")
	}
}
