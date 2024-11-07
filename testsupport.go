package automerge_s3_sync

import (
	"errors"
	"reflect"
	"slices"
	"testing"
)

func AssertEqual[k any](t *testing.T, actual, expected k) bool {
	t.Helper()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("actual = %v, expected = %v", actual, expected)
		return false
	}
	return true
}

func MustAssertEqual[k any](t *testing.T, actual, expected k) {
	t.Helper()
	if !AssertEqual(t, actual, expected) {
		t.FailNow()
	}
}

func AssertContains[E comparable](t *testing.T, container []E, item E) bool {
	t.Helper()
	if !slices.Contains(container, item) {
		t.Errorf("item %v not found in container %v", item, container)
		return false
	}
	return true
}

func AssertErrorEqual(t *testing.T, actual error, expected string) bool {
	t.Helper()
	if actual == nil {
		t.Errorf("expected an error, got nil")
		return false
	} else if actual.Error() != expected {
		t.Errorf("expected error: %v to equal %s", actual.Error(), expected)
		return false
	}
	return true
}

func AssertErrorIs(t *testing.T, actual error, expected error) bool {
	t.Helper()
	if actual == nil {
		t.Errorf("expected an error, got nil")
		return false
	} else if !errors.Is(actual, expected) {
		t.Errorf("expected error: %v to be a %s", actual.Error(), expected)
		return false
	}
	return true
}
