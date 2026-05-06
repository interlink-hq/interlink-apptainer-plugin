package apptainer

import (
	"context"
	"strings"
	"testing"
)

// TestGenerateProbeScript_HTTPProbePassesTimeout is a regression test for the
// bug where executeHTTPProbe was called without the "$timeout" argument, causing
// every HTTP probe to fail with "timeout: invalid time interval 'container-name'".
//
// This was the root cause of the e2e probes test timing out: the readiness and
// startup probes always failed because the timeout value received by
// executeHTTPProbe was the container name string instead of the numeric timeout.
func TestGenerateProbeScript_HTTPProbePassesTimeout(t *testing.T) {
	ctx := context.Background()
	cfg := testApptainerConfig()

	readiness := []ProbeCommand{
		{
			Type: ProbeTypeHTTP,
			HTTPGetAction: &HTTPGetAction{
				Scheme: "HTTP",
				Host:   "localhost",
				Port:   8080,
				Path:   "/ready",
			},
			InitialDelaySeconds: 3,
			PeriodSeconds:       3,
			TimeoutSeconds:      5,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		},
	}
	startup := []ProbeCommand{
		{
			Type: ProbeTypeHTTP,
			HTTPGetAction: &HTTPGetAction{
				Scheme: "HTTP",
				Host:   "localhost",
				Port:   8080,
				Path:   "/startup",
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			TimeoutSeconds:      1,
			SuccessThreshold:    1,
			FailureThreshold:    5,
		},
	}
	liveness := []ProbeCommand{
		{
			Type: ProbeTypeHTTP,
			HTTPGetAction: &HTTPGetAction{
				Scheme: "HTTP",
				Host:   "localhost",
				Port:   8080,
				Path:   "/healthz",
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      1,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		},
	}

	script := generateProbeScript(ctx, cfg, "probe-test", "docker://python:3.11-alpine", readiness, liveness, startup)

	if script == "" {
		t.Fatal("generateProbeScript returned empty string")
	}

	// The critical invariant: every call to executeHTTPProbe must pass "$timeout"
	// before "$container_name".  We check that the generated script never has
	// the old broken pattern (without the timeout arg).
	brokenCall := `executeHTTPProbe "${probe_args[@]}" "$container_name"`
	if strings.Contains(script, brokenCall) {
		t.Errorf("generated probe script still contains the broken executeHTTPProbe call (missing $timeout arg):\n%s", script)
	}

	// Verify the correct pattern is present in both runProbe and runStartupProbe.
	correctCall := `executeHTTPProbe "${probe_args[@]}" "$timeout" "$container_name"`
	count := strings.Count(script, correctCall)
	if count < 2 {
		t.Errorf("expected at least 2 occurrences of correct executeHTTPProbe call (one in runProbe, one in runStartupProbe), found %d\nScript:\n%s", count, script)
	}
}

// TestGenerateProbeScript_HTTPProbeTimeout_NumericValue verifies that the
// TimeoutSeconds value from the probe spec is visible as the "$timeout" variable
// when executeHTTPProbe is called.  We do this by checking that the runProbe and
// runStartupProbe function bodies correctly assign `local timeout="$5"` (the 5th
// positional argument), so that "$timeout" is the numeric probe timeout when
// executeHTTPProbe is later called.
func TestGenerateProbeScript_HTTPProbeTimeout_NumericValue(t *testing.T) {
	ctx := context.Background()
	cfg := testApptainerConfig()

	probe := []ProbeCommand{
		{
			Type: ProbeTypeHTTP,
			HTTPGetAction: &HTTPGetAction{
				Scheme: "HTTP",
				Host:   "localhost",
				Port:   9090,
				Path:   "/health",
			},
			InitialDelaySeconds: 0,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		},
	}

	script := generateProbeScript(ctx, cfg, "my-container", "docker://alpine:latest", probe, nil, nil)

	// runProbe must declare `local timeout="$5"` so the 5th argument (the
	// numeric timeout) is available as $timeout when executeHTTPProbe is called.
	if !strings.Contains(script, `local timeout="$5"`) {
		t.Error("runProbe must declare 'local timeout=\"$5\"' for timeout to be available to executeHTTPProbe")
	}

	// The generated runProbe invocation passes the timeout as the 5th argument:
	//   runProbe "http" "my-container" 0 5 3 1 3 "readiness" 0 "http" "localhost" "9090" "/health" &
	// Verify the probe invocation line includes the numeric TimeoutSeconds value.
	if !strings.Contains(script, `runProbe "http" "my-container" 0 5 3 1 3 "readiness" 0`) {
		t.Errorf("runProbe invocation not found in probe script:\n%s", script)
	}
}
