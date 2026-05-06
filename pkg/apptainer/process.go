package apptainer

import (
	"context"
	"syscall"

	"github.com/containerd/containerd/log"
)

// isProcessAlive returns true when a process with the given PID exists and
// has not yet exited.  On Linux this is equivalent to checking whether
// /proc/<pid> is present; on other systems it relies on kill(pid, 0).
func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// syscall.Kill with signal 0 does not actually send a signal; it only
	// checks whether the process exists and the caller has permission to send
	// signals to it.  A nil error means the process is alive.
	err := syscall.Kill(pid, 0)
	return err == nil
}

// killProcessGroup sends SIGTERM to the process group that contains pid.
// Using a negative PID value in Kill targets the entire process group (POSIX).
// This ensures that child processes spawned by job.sh are also terminated.
func killProcessGroup(ctx context.Context, pid int) {
	if pid <= 0 {
		return
	}

	// First try SIGTERM so processes can clean up.
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		log.G(ctx).Warning("Could not get process group for PID ", pid, ": ", err, " — sending SIGTERM to PID directly")
		syscall.Kill(pid, syscall.SIGTERM)
		return
	}

	log.G(ctx).Infof("Sending SIGTERM to process group %d (PID %d)", pgid, pid)
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
		log.G(ctx).Warning("SIGTERM to process group failed: ", err, " — trying SIGTERM to PID directly")
		syscall.Kill(pid, syscall.SIGTERM)
	}
}
