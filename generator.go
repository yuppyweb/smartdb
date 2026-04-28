package smartdb

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	savepointNameRandLen = 8
)

type Generator interface {
	SavepointName() (string, error)
}

// generator generates unique savepoint names for nested transaction management.
// It uses a combination of timestamp and random bytes to create SQL-compliant names
// that prevent conflicts in concurrent savepoint creation scenarios.
type generator struct {
	reader func([]byte) (int, error)
}

// NewGenerator creates a new Generator with the specified Reader function for generating random bytes.
// If the Reader is nil, it defaults to crypto/rand.Read.
func NewGenerator(reader func([]byte) (int, error)) Generator {
	if reader == nil {
		reader = rand.Read
	}

	return &generator{
		reader: reader,
	}
}

// SavepointName generates a unique savepoint name that conforms to
// the regex pattern ^[a-zA-Z_][a-zA-Z0-9_]*$ and does not exceed 32 characters.
// The generated name format is: sp<timestamp_hex><random_hex>.
//
// Length guarantee:
// - sp prefix: 2 chars
// - UnixMicro in hex: max 14 chars (at year 4253)
// - uint64 random in hex: 16 chars (max)
// - Total: 2 + 14 + 16 = 32 chars maximum
//
// This ensures SQL compatibility across all major databases without escaping.
// Timestamp + random bytes ensure uniqueness across concurrent savepoint creation,
// which is critical for preventing SQL naming conflicts in nested transactions.
func (g *generator) SavepointName() (string, error) {
	randBytes := make([]byte, savepointNameRandLen)

	if _, err := g.reader(randBytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}

	randValue := binary.BigEndian.Uint64(randBytes)

	return fmt.Sprintf("sp%x%x", time.Now().UnixMicro(), randValue), nil
}

var _ Generator = (*generator)(nil)
