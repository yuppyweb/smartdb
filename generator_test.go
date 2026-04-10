package smartdb_test

import (
	"errors"
	"testing"

	"github.com/yuppyweb/smartdb"
)

func TestGenerator_SavepointName(t *testing.T) {
	t.Parallel()

	names := make(map[string]struct{})
	count := 10000

	gen := smartdb.NewGenerator(nil)

	for range count {
		name, err := gen.SavepointName()
		if err != nil {
			t.Fatalf("unexpected error generating savepoint name: %v", err)
		}

		if _, exists := names[name]; exists {
			t.Fatalf("duplicate savepoint name generated: %s", name)
		}

		names[name] = struct{}{}
	}

	if len(names) != count {
		t.Fatalf("expected %d unique names, got %d", count, len(names))
	}
}

func TestGenerator_SavepointName_Error(t *testing.T) {
	expectedErr := errors.New("test error")

	gen := smartdb.NewGenerator(func(b []byte) (int, error) {
		return 0, expectedErr
	})

	_, err := gen.SavepointName()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}
