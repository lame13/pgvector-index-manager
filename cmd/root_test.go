package cmd

import (
	"errors"
	"testing"
)

func TestExitCode(t *testing.T) {
	if got := ExitCode(nil); got != 0 {
		t.Fatalf("ExitCode(nil) = %d", got)
	}
	if got := ExitCode(errors.New("runtime")); got != 1 {
		t.Fatalf("ExitCode(runtime) = %d", got)
	}
	if got := ExitCode(ErrNothingToApply); got != 0 {
		t.Fatalf("ExitCode(nothing to apply) = %d", got)
	}
}
