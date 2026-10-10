package main

import (
	"testing"
	"time"
)

func TestEnvMillisDefaultsWhenUnset(t *testing.T) {
	t.Setenv("PVMBG_DELAY_TEST", "")
	if got := envMillis("PVMBG_DELAY_TEST", 500); got != 500*time.Millisecond {
		t.Fatalf("envMillis default = %s, want 500ms", got)
	}
}

func TestEnvMillisReadsConfiguredValue(t *testing.T) {
	t.Setenv("PVMBG_DELAY_TEST", "3000")
	if got := envMillis("PVMBG_DELAY_TEST", 500); got != 3*time.Second {
		t.Fatalf("envMillis = %s, want 3s", got)
	}
}

func TestRandomDurationIsFixedWhenBoundsEqual(t *testing.T) {
	for i := 0; i < 20; i++ {
		if got := randomDuration(3*time.Second, 3*time.Second); got != 3*time.Second {
			t.Fatalf("randomDuration = %s, want exactly 3s", got)
		}
	}
}
