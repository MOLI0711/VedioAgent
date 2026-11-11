package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"unicode/utf8"
)

// UTF8ByteCounter is the explicit fallback profile for a hosted embedding
// model whose exact tokenizer is not exposed. Its values are byte lengths,
// never claims about model token counts.
type UTF8ByteCounter struct{}

func (UTF8ByteCounter) Count(text string) (int, error) {
	if !utf8.ValidString(text) {
		return 0, errors.New("chunk text is not valid UTF-8")
	}
	return len(text), nil
}

func (UTF8ByteCounter) Fingerprint() []byte {
	sum := sha256.Sum256([]byte("mirrorcore:utf8-byte-length:v1"))
	return sum[:]
}

func (c UTF8ByteCounter) FingerprintHex() string { return hex.EncodeToString(c.Fingerprint()) }
