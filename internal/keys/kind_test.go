package keys_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

const (
	persistedDelete = 0
	persistedValue  = 1

	invalidKindValueA keys.Kind = 5
	invalidKindValueB keys.Kind = 6
)

func maxKind(a, b keys.Kind) keys.Kind {
	if a < b {
		return b
	}
	return a
}

func TestKindValues(t *testing.T) {
	if persistedDelete != keys.Delete {
		t.Errorf("test=persisted-kind-delete got=%d want=%d", keys.Delete, persistedDelete)
	}
	if persistedValue != keys.Value {
		t.Errorf("test=persisted-kind-value got=%d want=%d", keys.Value, persistedValue)
	}
	if maxKindValue := maxKind(keys.Delete, keys.Value); keys.Seek != maxKindValue {
		t.Errorf("test=seek got=%d want=%d", keys.Seek, maxKindValue)
	}
}

func TestKindString(t *testing.T) {
	if d, v := keys.Delete.String(), keys.Value.String(); d == v {
		t.Errorf("test=kind-string keys.Delete=%q keys.Value=%q", d, v)
	}
	if a, b := invalidKindValueA.String(), invalidKindValueB.String(); a == b {
		t.Errorf("test=invalid-kind-string a=%q b=%q", a, b)
	}
	if d, a := keys.Delete.String(), invalidKindValueA.String(); d == a {
		t.Errorf("test=kind-delete-invalid-string keys.Delete=%q a=%q", d, a)
	}
	if v, b := keys.Value.String(), invalidKindValueB.String(); v == b {
		t.Errorf("test=kind-value-invalid-string keys.Value=%q a=%q", v, b)
	}
}
