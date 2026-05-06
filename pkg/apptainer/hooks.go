package apptainer

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"al.essio.dev/pkg/shellescape"
	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// translateLifecycleHook converts a Kubernetes LifecycleHandler into an internal
// LifecycleHookSpec.  Returns nil when the handler is nil or unsupported.
func translateLifecycleHook(handler *v1.LifecycleHandler) *LifecycleHookSpec {
	if handler == nil {
		return nil
	}

	if handler.Exec != nil && len(handler.Exec.Command) > 0 {
		return &LifecycleHookSpec{
			Type:        LifecycleHookTypeExec,
			ExecCommand: handler.Exec.Command,
		}
	}

	if handler.HTTPGet != nil {
		if handler.HTTPGet.Port.Type == intstr.String {
			log.G(context.Background()).Warningf("preStop httpGet hook uses a named port (%q) which cannot be resolved in the apptainer context; hook will be skipped", handler.HTTPGet.Port.StrVal)
			return nil
		}
		scheme := strings.ToLower(string(handler.HTTPGet.Scheme))
		if scheme == "" {
			scheme = "http"
		}
		host := handler.HTTPGet.Host
		if host == "" {
			host = "localhost"
		}
		path := handler.HTTPGet.Path
		if path == "" {
			path = "/"
		}
		return &LifecycleHookSpec{
			Type: LifecycleHookTypeHTTPGet,
			HTTPGet: &LifecycleHTTPGetSpec{
				Scheme: scheme,
				Host:   host,
				Port:   handler.HTTPGet.Port.IntVal,
				Path:   path,
			},
		}
	}

	return nil
}

// generatePreStopTrap generates the shell-script fragment that installs a
// SIGTERM trap in job.sh.  When the job receives SIGTERM the trap runs each
// container's preStop lifecycle hook before forwarding the signal to the
// running container processes.
//
// The returned string is empty when no container has a preStop hook.
func generatePreStopTrap(config ApptainerConfig, commands []ContainerCommand) string {
	type entry struct {
		name      string
		hook      *LifecycleHookSpec
		imageName string
	}
	var entries []entry
	for _, cmd := range commands {
		if cmd.isInitContainer || cmd.preStopHook == nil {
			continue
		}
		entries = append(entries, entry{
			name:      cmd.containerName,
			hook:      cmd.preStopHook,
			imageName: cmd.containerImage,
		})
	}
	if len(entries) == 0 {
		return ""
	}

	var sb strings.Builder

	sb.WriteString("\n# PreStop lifecycle hooks — executed when the job receives SIGTERM\n")
	sb.WriteString("preStopTrap() {\n")
	sb.WriteString(`  printf "%s\n" "$(date -Is --utc) Received SIGTERM: running preStop lifecycle hooks..."` + "\n")

	for _, e := range entries {
		sb.WriteString(fmt.Sprintf(
			`  printf "%%s\n" "$(date -Is --utc) Running preStop hook for container %s..."`,
			e.name,
		) + "\n")

		outFile := fmt.Sprintf(`"${workingPath}/prestop-%s.out"`, e.name)

		switch e.hook.Type {
		case LifecycleHookTypeExec:
			quotedArgs := make([]string, len(e.hook.ExecCommand))
			for i, arg := range e.hook.ExecCommand {
				quotedArgs[i] = shellescape.Quote(arg)
			}
			if e.imageName != "" && config.ApptainerPath != "" {
				parts := []string{shellescape.Quote(config.ApptainerPath), "exec"}
				for _, opt := range config.ApptainerDefaultOptions {
					parts = append(parts, shellescape.Quote(opt))
				}
				parts = append(parts, shellescape.Quote(e.imageName), "timeout", "30")
				parts = append(parts, quotedArgs...)
				sb.WriteString(fmt.Sprintf("  %s >> %s 2>&1 || true\n",
					strings.Join(parts, " "), outFile))
			} else {
				sb.WriteString(fmt.Sprintf("  timeout 30 %s >> %s 2>&1 || true\n",
					strings.Join(quotedArgs, " "), outFile))
			}

		case LifecycleHookTypeHTTPGet:
			url := fmt.Sprintf("%s://%s:%d%s",
				e.hook.HTTPGet.Scheme,
				e.hook.HTTPGet.Host,
				e.hook.HTTPGet.Port,
				e.hook.HTTPGet.Path,
			)
			sb.WriteString(fmt.Sprintf("  curl -f -s --max-time 10 %s >> %s 2>&1 || true\n",
				shellescape.Quote(url), outFile))
		}
	}

	sb.WriteString(`  printf "%s\n" "$(date -Is --utc) preStop hooks completed, terminating containers..."` + "\n")
	sb.WriteString("  for pidCtn in ${pidCtns} ; do\n")
	sb.WriteString("    pid=\"${pidCtn%:*}\"\n")
	sb.WriteString("    ctn=\"${pidCtn#*:}\"\n")
	sb.WriteString(`    printf "%s\n" "$(date -Is --utc) Sending SIGTERM to container ${ctn} pid ${pid}..."` + "\n")
	sb.WriteString("    kill \"${pid}\" 2>/dev/null || true\n")
	sb.WriteString("  done\n")
	sb.WriteString("  wait\n")
	sb.WriteString(`  printf "%s\n" "$(date -Is --utc) All containers terminated."` + "\n")
	sb.WriteString("}\n")
	sb.WriteString("trap preStopTrap SIGTERM\n")

	return sb.String()
}

// hookTmpBindMountArg is the shell token for the --bind argument that shares
// a dedicated working-directory sub-folder as /tmp between the postStart hook
// invocation and the main container launch.
const hookTmpBindMountArg = `"${workingPath}/hook-tmp:/tmp"`

// reTmpMount matches a singularity/apptainer bind-spec whose container
// destination is exactly /tmp.
var reTmpMount = regexp.MustCompile(`([^:\s]+):/tmp(?::|[\s]|$)`)

// findTmpBindHostPath scans a runtime-command slice for an existing --bind
// spec whose container destination is /tmp.  Returns the host path or "".
func findTmpBindHostPath(runtimeCmd []string) string {
	for _, elem := range runtimeCmd {
		if m := reTmpMount.FindStringSubmatch(elem); m != nil {
			return m[1]
		}
	}
	return ""
}

// generatePostStartScript generates a shell-script fragment that runs a
// container's postStart lifecycle hook synchronously before the container is
// launched.
func generatePostStartScript(config ApptainerConfig, cmd ContainerCommand) string {
	if cmd.isInitContainer || cmd.postStartHook == nil {
		return ""
	}

	imageName := cmd.containerImage
	outFile := fmt.Sprintf(`"${workingPath}/run-%s.out"`, cmd.containerName)

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("\n# postStart lifecycle hook for container %s\n", cmd.containerName))

	tmpBindArg := hookTmpBindMountArg
	if existingHostPath := findTmpBindHostPath(cmd.runtimeCommand); existingHostPath != "" {
		tmpBindArg = fmt.Sprintf("%s:/tmp", shellescape.Quote(existingHostPath))
	} else {
		sb.WriteString(`mkdir -p "${workingPath}/hook-tmp"` + "\n")
	}

	sb.WriteString(fmt.Sprintf(
		`printf "%%s\n" "$(date -Is --utc) Running postStart hook for container %s..." >> %s 2>&1`+"\n",
		cmd.containerName, outFile,
	))

	switch cmd.postStartHook.Type {
	case LifecycleHookTypeExec:
		quotedArgs := make([]string, len(cmd.postStartHook.ExecCommand))
		for i, arg := range cmd.postStartHook.ExecCommand {
			quotedArgs[i] = shellescape.Quote(arg)
		}
		if imageName != "" && config.ApptainerPath != "" {
			parts := []string{shellescape.Quote(config.ApptainerPath), "exec"}
			for _, opt := range config.ApptainerDefaultOptions {
				parts = append(parts, shellescape.Quote(opt))
			}
			parts = append(parts, "--bind", tmpBindArg)
			parts = append(parts, shellescape.Quote(imageName), "timeout", "30")
			parts = append(parts, quotedArgs...)
			sb.WriteString(fmt.Sprintf("%s >> %s 2>&1 || true\n",
				strings.Join(parts, " "), outFile))
		} else {
			sb.WriteString(fmt.Sprintf("timeout 30 %s >> %s 2>&1 || true\n",
				strings.Join(quotedArgs, " "), outFile))
		}

	case LifecycleHookTypeHTTPGet:
		url := fmt.Sprintf("%s://%s:%d%s",
			cmd.postStartHook.HTTPGet.Scheme,
			cmd.postStartHook.HTTPGet.Host,
			cmd.postStartHook.HTTPGet.Port,
			cmd.postStartHook.HTTPGet.Path,
		)
		sb.WriteString(fmt.Sprintf("curl -f -s --max-time 10 %s >> %s 2>&1 || true\n",
			shellescape.Quote(url), outFile))
	}

	sb.WriteString(fmt.Sprintf(
		`printf "%%s\n" "$(date -Is --utc) postStart hook for container %s completed." >> %s 2>&1`+"\n",
		cmd.containerName, outFile,
	))

	return sb.String()
}

// injectTmpBindMount inserts "--bind" hookTmpBindMountArg before the last
// element (the container image) in a runtime command slice so the main
// container sees the same /tmp as the postStart hook.
func injectTmpBindMount(runtimeCmd []string) []string {
	if len(runtimeCmd) == 0 {
		log.G(context.Background()).Warning("injectTmpBindMount: runtimeCmd is empty; skipping /tmp bind mount injection")
		return runtimeCmd
	}
	if findTmpBindHostPath(runtimeCmd) != "" {
		return runtimeCmd
	}
	result := make([]string, 0, len(runtimeCmd)+2)
	result = append(result, runtimeCmd[:len(runtimeCmd)-1]...)
	result = append(result, "--bind", hookTmpBindMountArg)
	result = append(result, runtimeCmd[len(runtimeCmd)-1])
	return result
}
