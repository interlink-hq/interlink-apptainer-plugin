package apptainer

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// isProcessAlive
// ---------------------------------------------------------------------------

// TestIsProcessAlive_CurrentProcess verifies that the running test process
// reports as alive.
func TestIsProcessAlive_CurrentProcess(t *testing.T) {
	pid := os.Getpid()
	if !isProcessAlive(pid) {
		t.Errorf("isProcessAlive(%d) = false; the current process must be alive", pid)
	}
}

// TestIsProcessAlive_InvalidPID verifies that pid <= 0 is never alive.
func TestIsProcessAlive_InvalidPID(t *testing.T) {
	for _, pid := range []int{0, -1, -100} {
		if isProcessAlive(pid) {
			t.Errorf("isProcessAlive(%d) = true; non-positive PID must never be alive", pid)
		}
	}
}

// TestIsProcessAlive_DeadProcess starts a short-lived subprocess, waits for it
// to exit, then asserts that isProcessAlive returns false.
func TestIsProcessAlive_DeadProcess(t *testing.T) {
	trueBin := "/bin/true"
	if _, err := os.Stat(trueBin); err != nil {
		t.Skipf("%s not available: %v", trueBin, err)
	}

	cmd := exec.Command(trueBin)
	if err := cmd.Start(); err != nil {
		t.Fatalf("could not start %s: %v", trueBin, err)
	}
	pid := cmd.Process.Pid

	// Wait for the process to exit (reaps it so the OS removes the PID).
	_ = cmd.Wait()

	// Give the OS a moment to clean up.
	time.Sleep(50 * time.Millisecond)
	if isProcessAlive(pid) {
		t.Errorf("isProcessAlive(%d) = true for an already-exited process", pid)
	}
}

// ---------------------------------------------------------------------------
// getExitCode
// ---------------------------------------------------------------------------

// TestGetExitCode_FromRunFile verifies that when the run-<container>.status
// file contains "0", getExitCode returns 0 without error.
func TestGetExitCode_FromRunFile(t *testing.T) {
	dir := t.TempDir()

	statusPath := filepath.Join(dir, "run-mycontainer.status")
	if err := os.WriteFile(statusPath, []byte("0\n"), 0644); err != nil {
		t.Fatalf("could not write status file: %v", err)
	}

	code, err := getExitCode(context.Background(), dir, "mycontainer", "1", "")
	if err != nil {
		t.Fatalf("getExitCode() unexpected error: %v", err)
	}
	if code != 0 {
		t.Errorf("getExitCode() = %d, want 0", code)
	}
}

// TestGetExitCode_NonZeroExitCode verifies that a non-zero exit code in the
// status file is read correctly.
func TestGetExitCode_NonZeroExitCode(t *testing.T) {
	dir := t.TempDir()

	statusPath := filepath.Join(dir, "run-mycontainer.status")
	if err := os.WriteFile(statusPath, []byte("42"), 0644); err != nil {
		t.Fatalf("could not write status file: %v", err)
	}

	code, err := getExitCode(context.Background(), dir, "mycontainer", "1", "")
	if err != nil {
		t.Fatalf("getExitCode() unexpected error: %v", err)
	}
	if code != 42 {
		t.Errorf("getExitCode() = %d, want 42", code)
	}
}

// TestGetExitCode_FallbackWhenMissing verifies that when neither
// run-<container>.status nor init-<container>.status exists, the fallback code
// is returned and the file is created.
func TestGetExitCode_FallbackWhenMissing(t *testing.T) {
	dir := t.TempDir()

	code, err := getExitCode(context.Background(), dir, "mycontainer", "7", "")
	if err != nil {
		t.Fatalf("getExitCode() unexpected error: %v", err)
	}
	if code != 7 {
		t.Errorf("getExitCode() = %d, want fallback 7", code)
	}

	// The function should have written the fallback to the init- file.
	statusPath := filepath.Join(dir, "init-mycontainer.status")
	if _, statErr := os.Stat(statusPath); statErr != nil {
		t.Errorf("expected fallback status file to be created at %s: %v", statusPath, statErr)
	}
}

// TestGetExitCode_InitContainerFallback verifies that when the run- file is
// absent but the init- file exists, its value is returned.
func TestGetExitCode_InitContainerFallback(t *testing.T) {
	dir := t.TempDir()

	initStatusPath := filepath.Join(dir, "init-mycontainer.status")
	if err := os.WriteFile(initStatusPath, []byte("3"), 0644); err != nil {
		t.Fatalf("could not write init status file: %v", err)
	}

	code, err := getExitCode(context.Background(), dir, "mycontainer", "1", "")
	if err != nil {
		t.Fatalf("getExitCode() unexpected error: %v", err)
	}
	if code != 3 {
		t.Errorf("getExitCode() = %d, want 3", code)
	}
}

// ---------------------------------------------------------------------------
// checkIfJidExists — status-focused test
// ---------------------------------------------------------------------------

// TestCheckIfJidExists_StatusFocus ensures that the JID map look-up used by
// StatusHandler works correctly for present and absent entries.
func TestCheckIfJidExists_StatusFocus(t *testing.T) {
	ctx := context.Background()
	jids := map[string]*JidStruct{
		"pod-uid-abc": {
			PodUID: "pod-uid-abc",
			JID:    strconv.Itoa(os.Getpid()), // current process → definitely alive
		},
	}

	if !checkIfJidExists(ctx, &jids, "pod-uid-abc") {
		t.Error("checkIfJidExists returned false for an existing entry")
	}
	if checkIfJidExists(ctx, &jids, "pod-uid-xyz") {
		t.Error("checkIfJidExists returned true for a non-existing entry")
	}
}
