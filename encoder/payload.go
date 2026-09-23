// Package encoder implements the client-side payload protection used by the
// Dataflow SDK: AES-256-GCM over captured data with a key derived from a
// user-supplied secret via PBKDF2-SHA256. The secret never leaves the host
// process; servers and dashboards only ever see ciphertext plus the salt
// needed to re-derive the key.
package encoder

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"

	"crypto/pbkdf2"
)

const (
	// KeyLen is the AES-256 key size in bytes.
	KeyLen = 32
	// IVLen is the GCM nonce size in bytes.
	IVLen = 12
	// Iterations is the PBKDF2 work factor.
	Iterations = 10000
)

// SaltFromHex decodes a hex-encoded salt, or generates a fresh random one
// when raw is empty.
func SaltFromHex(raw string) ([]byte, error) {
	if raw == "" {
		salt := make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			return nil, fmt.Errorf("dataflow/encoder: generate salt: %w", err)
		}
		return salt, nil
	}
	salt, err := hex.DecodeString(raw)
	if err != nil || len(salt) == 0 {
		return nil, errors.New("dataflow/encoder: invalid hex salt")
	}
	return salt, nil
}

// DeriveKey stretches secret with salt into an AES-256 key.
func DeriveKey(secret string, salt []byte) ([]byte, error) {
	if secret == "" {
		return nil, errors.New("dataflow/encoder: empty secret")
	}
	return pbkdf2.Key(sha256.New, secret, salt, Iterations, KeyLen)
}

// Encrypt seals plaintext under key, returning the ciphertext and a fresh
// random IV.
func Encrypt(key, plaintext []byte) ([]byte, []byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}
	iv := make([]byte, IVLen)
	if _, err := rand.Read(iv); err != nil {
		return nil, nil, fmt.Errorf("dataflow/encoder: generate iv: %w", err)
	}
	return gcm.Seal(nil, iv, plaintext, nil), iv, nil
}

// Decrypt opens ciphertext under key using iv.
func Decrypt(key, ciphertext, iv []byte) ([]byte, error) {
	if len(iv) != IVLen {
		return nil, errors.New("dataflow/encoder: invalid iv length")
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		return nil, errors.New("dataflow/encoder: decrypt failed (wrong key?)")
	}
	return plaintext, nil
}

// Verify reports whether the supplied secret matches the key that was used
// to produce a test vector — used by dashboards to validate an encryption
// key without decrypting real payloads.
func Verify(secret string, salt, key []byte) bool {
	derived, err := DeriveKey(secret, salt)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(derived, key) == 1
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != KeyLen {
		return nil, errors.New("dataflow/encoder: key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("dataflow/encoder: %w", err)
	}
	return cipher.NewGCM(block)
}
