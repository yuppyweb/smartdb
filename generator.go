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

// Generator generates unique savepoint names for nested transaction management.
// It uses a combination of timestamp and random bytes to create SQL-compliant names
// that prevent conflicts in concurrent savepoint creation scenarios.
type Generator struct {
	reader func([]byte) (int, error)
}

// NewGenerator creates a new Generator with the specified Reader function for generating random bytes.
// If the Reader is nil, it defaults to crypto/rand.Read.
func NewGenerator(reader func([]byte) (int, error)) *Generator {
	if reader == nil {
		reader = rand.Read
	}

	return &Generator{
		reader: reader,
	}
}

// SavepointName generates a unique savepoint name that conforms to
// the regex pattern ^[a-zA-Z_][a-zA-Z0-9_]*$ and does not exceed 32 characters.
// The generated name format is: sp_<timestamp_hex>_<random_hex>.
// Timestamp + random bytes ensure uniqueness across concurrent savepoint creation,
// which is critical for preventing SQL naming conflicts in nested transactions.
func (g *Generator) SavepointName() (string, error) {
	randBytes := make([]byte, savepointNameRandLen)

	if _, err := g.reader(randBytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}

	randValue := binary.BigEndian.Uint64(randBytes)

	return fmt.Sprintf("sp%x%x", time.Now().UnixMicro(), randValue), nil
}
