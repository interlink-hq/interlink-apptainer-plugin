package apptainer

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"

	"go.opentelemetry.io/otel/trace"
)

// translateKubernetesProbes converts Kubernetes probe specifications to the
// internal ProbeCommand format for all three probe types.
func translateKubernetesProbes(ctx context.Context, container v1.Container) ([]ProbeCommand, []ProbeCommand, []ProbeCommand) {
	var readinessProbes, livenessProbes, startupProbes []ProbeCommand
	span := trace.SpanFromContext(ctx)

	if container.StartupProbe != nil {
		probe := translateSingleProbe(ctx, container.StartupProbe)
		if probe != nil {
			startupProbes = append(startupProbes, *probe)
			span.AddEvent("Translated startup probe for container " + container.Name)
		}
	}

	if container.ReadinessProbe != nil {
		probe := translateSingleProbe(ctx, container.ReadinessProbe)
		if probe != nil {
			readinessProbes = append(readinessProbes, *probe)
			span.AddEvent("Translated readiness probe for container " + container.Name)
		}
	}

	if container.LivenessProbe != nil {
		probe := translateSingleProbe(ctx, container.LivenessProbe)
		if probe != nil {
			livenessProbes = append(livenessProbes, *probe)
			span.AddEvent("Translated liveness probe for container " + container.Name)
		}
	}

	return readinessProbes, livenessProbes, startupProbes
}

// translateSingleProbe converts a single Kubernetes probe to the internal format.
func translateSingleProbe(ctx context.Context, k8sProbe *v1.Probe) *ProbeCommand {
	if k8sProbe == nil {
		return nil
	}

	probe := &ProbeCommand{
		InitialDelaySeconds: k8sProbe.InitialDelaySeconds,
		PeriodSeconds:       k8sProbe.PeriodSeconds,
		TimeoutSeconds:      k8sProbe.TimeoutSeconds,
		SuccessThreshold:    k8sProbe.SuccessThreshold,
		FailureThreshold:    k8sProbe.FailureThreshold,
	}

	// Apply defaults.
	if probe.PeriodSeconds == 0 {
		probe.PeriodSeconds = 10
	}
	if probe.TimeoutSeconds == 0 {
		probe.TimeoutSeconds = 1
	}
	if probe.SuccessThreshold == 0 {
		probe.SuccessThreshold = 1
	}
	if probe.FailureThreshold == 0 {
		probe.FailureThreshold = 3
	}

	if k8sProbe.HTTPGet != nil {
		probe.Type = ProbeTypeHTTP
		probe.HTTPGetAction = &HTTPGetAction{
			Path:   k8sProbe.HTTPGet.Path,
			Port:   k8sProbe.HTTPGet.Port.IntVal,
			Host:   k8sProbe.HTTPGet.Host,
			Scheme: string(k8sProbe.HTTPGet.Scheme),
		}
		if probe.HTTPGetAction.Scheme == "" {
			probe.HTTPGetAction.Scheme = "HTTP"
		}
		if probe.HTTPGetAction.Path == "" {
			probe.HTTPGetAction.Path = "/"
		}
		return probe
	}

	if k8sProbe.Exec != nil {
		probe.Type = ProbeTypeExec
		probe.ExecAction = &ExecAction{
			Command: k8sProbe.Exec.Command,
		}
		return probe
	}

	log.G(ctx).Warning("Unsupported probe type (only HTTP and Exec are supported)")
	return nil
}

// buildProbeArgs constructs the shell argument string for a probe command.
func buildProbeArgs(probe ProbeCommand) string {
	if probe.Type == ProbeTypeHTTP {
		scheme := strings.ToLower(probe.HTTPGetAction.Scheme)
		host := probe.HTTPGetAction.Host
		if host == "" {
			host = "localhost"
		}
		return fmt.Sprintf(`"%s" "%s" "%d" "%s"`,
			scheme, host, probe.HTTPGetAction.Port, probe.HTTPGetAction.Path)
	} else if probe.Type == ProbeTypeExec {
		quotedArgs := make([]string, len(probe.ExecAction.Command))
		for i, arg := range probe.ExecAction.Command {
			quotedArgs[i] = fmt.Sprintf(`"%s"`, strings.ReplaceAll(arg, `"`, `\"`))
		}
		return strings.Join(quotedArgs, " ")
	}
	return ""
}

// generateProbeScript generates the shell script fragment for probe execution.
func generateProbeScript(ctx context.Context, config ApptainerConfig, containerName string, imageName string, readinessProbes []ProbeCommand, livenessProbes []ProbeCommand, startupProbes []ProbeCommand) string {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("Generating probe script for container " + containerName)

	if len(readinessProbes) == 0 && len(livenessProbes) == 0 && len(startupProbes) == 0 {
		return ""
	}

	var scriptBuilder strings.Builder

	scriptBuilder.WriteString(`
# Probe execution functions
executeHTTPProbe() {
    local scheme="$1"
    local host="$2"
    local port="$3"
    local path="$4"
    local timeout="$5"
    local container_name="$6"

    if [ -z "$host" ] || [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
        host="localhost"
    fi

    url="${scheme,,}://${host}:${port}${path}"
    timeout "${timeout}" curl -f -s "$url" &> /dev/null
    return $?
}

executeExecProbe() {
    local timeout="$1"
    local container_name="$2"
    shift 2
    local command=("$@")
    `)
	scriptBuilder.WriteString(fmt.Sprintf(`"%s" exec`, config.ApptainerPath))
	for _, opt := range config.ApptainerDefaultOptions {
		scriptBuilder.WriteString(fmt.Sprintf(` "%s"`, opt))
	}
	scriptBuilder.WriteString(fmt.Sprintf(` "%s" timeout "${timeout}" "${command[@]}"
    return $?
}`, imageName))

	scriptBuilder.WriteString(`
runProbe() {
    local probe_type="$1"
    local container_name="$2"
    local initial_delay="$3"
    local period="$4"
    local timeout="$5"
    local success_threshold="$6"
    local failure_threshold="$7"
    local probe_name="$8"
    local probe_index="$9"
    shift 9
    local probe_args=("$@")

    local probe_status_file="${workingPath}/${probe_name}-probe-${container_name}-${probe_index}.status"
    local probe_timestamp_file="${workingPath}/${probe_name}-probe-${container_name}-${probe_index}.timestamp"

    printf "%s\n" "$(date -Is --utc) Starting ${probe_name} probe for container ${container_name}..."
    echo "UNKNOWN" > "$probe_status_file"
    date -Is --utc > "$probe_timestamp_file"

    if [ "$initial_delay" -gt 0 ]; then
        printf "%s\n" "$(date -Is --utc) Waiting ${initial_delay}s before starting ${probe_name} probe..."
        sleep "$initial_delay"
    fi

    local consecutive_successes=0
    local consecutive_failures=0
    local probe_ready=false

    while true; do
        date -Is --utc > "$probe_timestamp_file"

        if [ "$probe_type" = "http" ]; then
            executeHTTPProbe "${probe_args[@]}" "$timeout" "$container_name"
        elif [ "$probe_type" = "exec" ]; then
            executeExecProbe "$timeout" "$container_name" "${probe_args[@]}"
        fi

        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            consecutive_successes=$((consecutive_successes + 1))
            consecutive_failures=0
            if [ $consecutive_successes -ge $success_threshold ]; then
                echo "SUCCESS" > "$probe_status_file"
                probe_ready=true
                if [ "$probe_name" = "readiness" ]; then
                    return 0
                fi
            fi
        else
            consecutive_failures=$((consecutive_failures + 1))
            consecutive_successes=0
            printf "%s\n" "$(date -Is --utc) ${probe_name} probe failed for ${container_name} (${consecutive_failures}/${failure_threshold})"
            echo "FAILURE" > "$probe_status_file"
            probe_ready=false
            if [ $consecutive_failures -ge $failure_threshold ]; then
                echo "FAILED_THRESHOLD" > "$probe_status_file"
                if [ "$probe_name" = "readiness" ]; then
                    exit 1
                fi
            fi
        fi

        sleep "$period"
    done

    return 0
}

shutDownContainersOnProbeFail() {
  for pidCtn in ${pidCtns} ; do
    pid="${pidCtn%:*}"
    ctn="${pidCtn#*:}"
    printf "%s\n" "$(date -Is --utc) Container ${ctn} pid ${pid} killed for failed probes."
    kill "${pid}"
    printf "%s\n" "1" > "${workingPath}/run-${ctn}.status"
    waitFileExist "${workingPath}/run-${ctn}.status"
  done
}

runStartupProbe() {
    local probe_type="$1"
    local container_name="$2"
    local initial_delay="$3"
    local period="$4"
    local timeout="$5"
    local success_threshold="$6"
    local failure_threshold="$7"
    local probe_name="$8"
    local probe_index="$9"
    shift 9
    local probe_args=("$@")

    local probe_status_file="${workingPath}/${probe_name}-probe-${container_name}-${probe_index}.status"

    printf "%s\n" "$(date -Is --utc) Starting ${probe_name} probe for container ${container_name}..."
    echo "RUNNING" > "$probe_status_file"

    if [ "$initial_delay" -gt 0 ]; then
        sleep "$initial_delay"
    fi

    local consecutive_successes=0
    local consecutive_failures=0

    while true; do
        if [ "$probe_type" = "http" ]; then
            executeHTTPProbe "${probe_args[@]}" "$timeout" "$container_name"
        elif [ "$probe_type" = "exec" ]; then
            executeExecProbe "$timeout" "$container_name" "${probe_args[@]}"
        fi

        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            consecutive_successes=$((consecutive_successes + 1))
            consecutive_failures=0
            if [ $consecutive_successes -ge $success_threshold ]; then
                echo "SUCCESS" > "$probe_status_file"
                return 0
            fi
        else
            consecutive_failures=$((consecutive_failures + 1))
            consecutive_successes=0
            if [ $consecutive_failures -ge $failure_threshold ]; then
                echo "FAILED_THRESHOLD" > "$probe_status_file"
                exit 1
            fi
        fi

        sleep "$period"
    done
}

waitForProbes() {
    local probe_name="$1"
    local container_name="$2"
    local probe_count="$3"

    if [ "$probe_count" -eq 0 ]; then
        return 0
    fi

    printf "%s\n" "$(date -Is --utc) Waiting for ${probe_name} probes to succeed for ${container_name}..."

    while true; do
        local all_probes_successful=true

        for i in $(seq 0 $((probe_count - 1))); do
            local probe_status_file="${workingPath}/${probe_name}-probe-${container_name}-${i}.status"
            if [ ! -f "$probe_status_file" ]; then
                all_probes_successful=false
                break
            fi
            local status=$(cat "$probe_status_file")
            if [ "$status" != "SUCCESS" ]; then
                if [ "$status" = "FAILED_THRESHOLD" ]; then
                    return 1
                fi
                all_probes_successful=false
                break
            fi
        done

        if [ "$all_probes_successful" = true ]; then
            return 0
        fi
        sleep 1
    done
}

`)

	for i, probe := range startupProbes {
		probeArgs := buildProbeArgs(probe)
		containerVarName := strings.ReplaceAll(containerName, "-", "_")
		scriptBuilder.WriteString(fmt.Sprintf(`
runStartupProbe "%s" "%s" %d %d %d %d %d "startup" %d %s &
STARTUP_PROBE_%s_%d_PID=$!
`, probe.Type, containerName, probe.InitialDelaySeconds, probe.PeriodSeconds,
			probe.TimeoutSeconds, probe.SuccessThreshold, probe.FailureThreshold, i, probeArgs, containerVarName, i))
	}

	if len(startupProbes) > 0 {
		scriptBuilder.WriteString(fmt.Sprintf(`
(
    waitForProbes "startup" "%s" %d
    if [ $? -eq 0 ]; then
`, containerName, len(startupProbes)))
	} else {
		scriptBuilder.WriteString(`
(
if true; then
`)
	}

	if len(readinessProbes) > 0 {
		for i, probe := range readinessProbes {
			probeArgs := buildProbeArgs(probe)
			containerVarName := strings.ReplaceAll(containerName, "-", "_")
			scriptBuilder.WriteString(fmt.Sprintf(`
      runProbe "%s" "%s" %d %d %d %d %d "readiness" %d %s &
      READINESS_PROBE_%s_%d_PID=$!
`, probe.Type, containerName, probe.InitialDelaySeconds, probe.PeriodSeconds,
				probe.TimeoutSeconds, probe.SuccessThreshold, probe.FailureThreshold, i, probeArgs, containerVarName, i))
		}

		scriptBuilder.WriteString(fmt.Sprintf(`
      waitForProbes "readiness" "%s" %d
      if [ $? -eq 0 ]; then
`, containerName, len(readinessProbes)))
	} else {
		scriptBuilder.WriteString(`
      if true; then
`)
	}

	if len(livenessProbes) == 0 {
		scriptBuilder.WriteString(fmt.Sprintf(`
          printf "%%s\n" "$(date -Is --utc) No liveness probes defined for %s."
`, containerName))
	} else {
		for i, probe := range livenessProbes {
			probeArgs := buildProbeArgs(probe)
			containerVarName := strings.ReplaceAll(containerName, "-", "_")
			scriptBuilder.WriteString(fmt.Sprintf(`
          runProbe "%s" "%s" %d %d %d %d %d "liveness" %d %s &
          LIVENESS_PROBE_%s_%d_PID=$!
`, probe.Type, containerName, probe.InitialDelaySeconds, probe.PeriodSeconds,
				probe.TimeoutSeconds, probe.SuccessThreshold, probe.FailureThreshold, i, probeArgs, containerVarName, i))
		}

		containerVarName := strings.ReplaceAll(containerName, "-", "_")
		scriptBuilder.WriteString(fmt.Sprintf(`
          wait $LIVENESS_PROBE_%s_0_PID
          if [ $? -ne 0 ]; then
              printf "%%s\n" "$(date -Is --utc) Liveness probe failed for %s, shutting down containers..."
              shutDownContainersOnProbeFail
          fi
`, containerVarName, containerName))
	}

	// Close all the if/fi blocks
	if len(readinessProbes) > 0 {
		scriptBuilder.WriteString(`
      fi
`)
	} else {
		scriptBuilder.WriteString(`
      fi
`)
	}

	if len(startupProbes) > 0 {
		scriptBuilder.WriteString(`
    fi
) &
`)
	} else {
		scriptBuilder.WriteString(`
) &
`)
	}

	return scriptBuilder.String()
}

// generateProbeCleanupScript generates a shell function that cleans up probe
// background processes when the job ends.
func generateProbeCleanupScript(containerName string, readinessProbes []ProbeCommand, livenessProbes []ProbeCommand, startupProbes []ProbeCommand) string {
	if len(readinessProbes) == 0 && len(livenessProbes) == 0 && len(startupProbes) == 0 {
		return ""
	}

	var sb strings.Builder
	containerVarName := strings.ReplaceAll(containerName, "-", "_")

	sb.WriteString("\n# Probe cleanup\n")
	sb.WriteString("cleanupProbes() {\n")

	for i := range startupProbes {
		sb.WriteString(fmt.Sprintf(
			`  [ -n "$STARTUP_PROBE_%s_%d_PID" ] && kill "$STARTUP_PROBE_%s_%d_PID" 2>/dev/null || true`+"\n",
			containerVarName, i, containerVarName, i))
	}
	for i := range readinessProbes {
		sb.WriteString(fmt.Sprintf(
			`  [ -n "$READINESS_PROBE_%s_%d_PID" ] && kill "$READINESS_PROBE_%s_%d_PID" 2>/dev/null || true`+"\n",
			containerVarName, i, containerVarName, i))
	}
	for i := range livenessProbes {
		sb.WriteString(fmt.Sprintf(
			`  [ -n "$LIVENESS_PROBE_%s_%d_PID" ] && kill "$LIVENESS_PROBE_%s_%d_PID" 2>/dev/null || true`+"\n",
			containerVarName, i, containerVarName, i))
	}

	sb.WriteString("}\n")
	sb.WriteString("trap cleanupProbes EXIT\n")

	return sb.String()
}

// storeProbeMetadata writes the number of each probe type to disk so that the
// status handler can assess readiness without re-reading the pod spec.
func storeProbeMetadata(path string, containerName string, readinessCount int, livenessCount int, startupCount int) error {
	metadataPath := path + "/probe-metadata-" + containerName + ".txt"
	content := fmt.Sprintf("readiness=%d\nliveness=%d\nstartup=%d\n", readinessCount, livenessCount, startupCount)
	return os.WriteFile(metadataPath, []byte(content), 0644)
}

// loadProbeMetadata reads the stored probe counts for a container.
func loadProbeMetadata(path string, containerName string) (int, int, int, error) {
	metadataPath := path + "/probe-metadata-" + containerName + ".txt"
	content, err := os.ReadFile(metadataPath)
	if err != nil {
		return 0, 0, 0, err
	}

	var readinessCount, livenessCount, startupCount int
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		count, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			continue
		}
		switch parts[0] {
		case "readiness":
			readinessCount = count
		case "liveness":
			livenessCount = count
		case "startup":
			startupCount = count
		}
	}

	return readinessCount, livenessCount, startupCount, nil
}

// checkContainerReadiness checks whether readiness probes have all succeeded.
func checkContainerReadiness(ctx context.Context, config ApptainerConfig, path string, containerName string, readinessCount int) bool {
	if readinessCount == 0 {
		return true
	}
	for i := 0; i < readinessCount; i++ {
		statusFile := fmt.Sprintf("%s/readiness-probe-%s-%d.status", path, containerName, i)
		content, err := os.ReadFile(statusFile)
		if err != nil {
			return false
		}
		if strings.TrimSpace(string(content)) != "SUCCESS" {
			return false
		}
	}
	return true
}

// checkContainerLiveness checks whether liveness probes are currently passing.
func checkContainerLiveness(ctx context.Context, config ApptainerConfig, path string, containerName string, livenessCount int) bool {
	if livenessCount == 0 {
		return true
	}
	for i := 0; i < livenessCount; i++ {
		statusFile := fmt.Sprintf("%s/liveness-probe-%s-%d.status", path, containerName, i)
		content, err := os.ReadFile(statusFile)
		if err != nil {
			return false
		}
		status := strings.TrimSpace(string(content))
		if status == "FAILED_THRESHOLD" {
			return false
		}
	}
	return true
}

// checkContainerStartupComplete checks whether startup probes have all succeeded.
func checkContainerStartupComplete(ctx context.Context, config ApptainerConfig, path string, containerName string, startupCount int) bool {
	if startupCount == 0 {
		return true
	}
	for i := 0; i < startupCount; i++ {
		statusFile := fmt.Sprintf("%s/startup-probe-%s-%d.status", path, containerName, i)
		content, err := os.ReadFile(statusFile)
		if err != nil {
			return false
		}
		if strings.TrimSpace(string(content)) != "SUCCESS" {
			return false
		}
	}
	return true
}


