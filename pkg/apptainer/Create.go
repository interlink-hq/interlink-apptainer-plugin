package apptainer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/containerd/containerd/log"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// SubmitHandler generates job.sh and executes it directly as a subprocess.
// 1 Pod = 1 direct process (no batch scheduler involved).
func (h *SidecarHandler) SubmitHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "Create", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	log.G(h.Ctx).Info("Apptainer Sidecar: received Submit call")
	statusCode := http.StatusOK
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	var data commonIL.RetrievedPodData
	var returnedJID CreateStruct
	var returnedJIDBytes []byte

	err = json.Unmarshal(bodyBytes, &data)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, http.StatusGatewayTimeout, err)
		return
	}

	containers := data.Pod.Spec.InitContainers
	containers = append(containers, data.Pod.Spec.Containers...)
	metadata := data.Pod.ObjectMeta
	filesPath := h.Config.DataRootFolder + data.Pod.Namespace + "-" + string(data.Pod.UID)

	var runtime_command_pod []ContainerCommand

	cpuLimit := int64(0)
	memoryLimit := int64(0)

	for i, container := range containers {
		log.G(h.Ctx).Info("- Beginning script generation for container " + container.Name)

		image := ""

		cpuLimitFloat := container.Resources.Limits.Cpu().AsApproximateFloat64()
		memoryLimitFromContainer, _ := container.Resources.Limits.Memory().AsInt64()
		cpuLimitFromContainer := int64(math.Ceil(cpuLimitFloat))

		if cpuLimitFromContainer == 0 {
			if cpuLimit == 0 {
				log.G(h.Ctx).Warning(errors.New("Max CPU resource not set for " + container.Name + ". Only 1 CPU will be used"))
				cpuLimit = 1
			}
		} else {
			if cpuLimitFromContainer > cpuLimit {
				cpuLimit = cpuLimitFromContainer
			}
		}

		if memoryLimitFromContainer == 0 {
			if memoryLimit == 0 {
				log.G(h.Ctx).Warning(errors.New("Max Memory resource not set for " + container.Name + ". Only 1MB will be used"))
				memoryLimit = 1024 * 1024
			}
		} else {
			if memoryLimitFromContainer > memoryLimit {
				memoryLimit = memoryLimitFromContainer
			}
		}

		mounts, err := prepareMounts(spanCtx, h.Config, &data, &container, filesPath)
		log.G(h.Ctx).Debug(mounts)
		if err != nil {
			statusCode = http.StatusInternalServerError
			h.handleError(spanCtx, w, http.StatusGatewayTimeout, err)
			os.RemoveAll(filesPath)
			return
		}

		envs := prepareEnvs(spanCtx, h.Config, data, container)
		image = prepareImage(spanCtx, h.Config, metadata, container.Image)
		commstr1 := prepareRuntimeCommand(h.Config, container, metadata)
		log.G(h.Ctx).Debug("-- Appending all commands together...")
		runtime_command := append(commstr1, envs...)
		runtime_command = append(runtime_command, mounts)
		runtime_command = append(runtime_command, image)

		isInit := i < len(data.Pod.Spec.InitContainers)

		span.SetAttributes(
			attribute.String("job.container"+strconv.Itoa(i)+".name", container.Name),
			attribute.Bool("job.container"+strconv.Itoa(i)+".isinit", isInit),
			attribute.StringSlice("job.container"+strconv.Itoa(i)+".envs", envs),
			attribute.String("job.container"+strconv.Itoa(i)+".image", image),
			attribute.StringSlice("job.container"+strconv.Itoa(i)+".command", container.Command),
			attribute.StringSlice("job.container"+strconv.Itoa(i)+".args", container.Args),
		)

		var readinessProbes, livenessProbes, startupProbes []ProbeCommand
		if h.Config.EnableProbes && !isInit {
			readinessProbes, livenessProbes, startupProbes = translateKubernetesProbes(spanCtx, container)
		}

		var preStopHook *LifecycleHookSpec
		var postStartHook *LifecycleHookSpec
		if !isInit && container.Lifecycle != nil {
			preStopHook = translateLifecycleHook(container.Lifecycle.PreStop)
			postStartHook = translateLifecycleHook(container.Lifecycle.PostStart)
		}

		runtime_command_pod = append(runtime_command_pod, ContainerCommand{
			runtimeCommand:   runtime_command,
			containerName:    container.Name,
			containerArgs:    container.Args,
			containerCommand: container.Command,
			isInitContainer:  isInit,
			readinessProbes:  readinessProbes,
			livenessProbes:   livenessProbes,
			startupProbes:    startupProbes,
			containerImage:   image,
			preStopHook:      preStopHook,
			postStartHook:    postStartHook,
		})
	}

	var scriptPath string

	if data.JobScript == "" {
		log.G(h.Ctx).Info("-- No custom job script provided, generating one...")
		scriptPath, err = produceApptainerScript(spanCtx, h.Config, data.Pod, filesPath, metadata, runtime_command_pod)
		if err != nil {
			log.G(h.Ctx).Error(err)
			os.RemoveAll(filesPath)
			return
		}
	} else {
		// Use the provided job script directly.
		pathFile, err := os.Create(filesPath + "/jobScript.sh")
		if err != nil {
			log.G(h.Ctx).Error("Unable to create file ", filesPath, "/jobScript.sh")
			log.G(h.Ctx).Error(err)
			span.AddEvent("Failed to create job script file")
			h.handleError(spanCtx, w, http.StatusInternalServerError, err)
			return
		}

		if err := os.Chmod(filesPath+"/jobScript.sh", 0770); err != nil {
			pathFile.Close()
			h.handleError(spanCtx, w, http.StatusInternalServerError, err)
			return
		}

		_, err = pathFile.Write([]byte(data.JobScript))
		pathFile.Close()
		if err != nil {
			log.G(h.Ctx).Error("Unable to write to file ", filesPath, "/jobScript.sh")
			h.handleError(spanCtx, w, http.StatusInternalServerError, err)
			return
		}
		scriptPath = pathFile.Name()
	}

	// Run job.sh directly instead of submitting to a batch scheduler.
	pid, err := runJobDirectly(h, spanCtx, data, scriptPath, filesPath)
	if err != nil {
		span.AddEvent("Failed to start the job process")
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, http.StatusGatewayTimeout, err)
		os.RemoveAll(filesPath)
		return
	}

	jid, err := storeJobInfo(h.Ctx, data.Pod, h.JIDs, pid, filesPath)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, http.StatusGatewayTimeout, err)
		os.RemoveAll(filesPath)
		return
	}

	span.AddEvent("Job successfully started with PID " + jid)
	returnedJID = CreateStruct{PodUID: string(data.Pod.UID), PodJID: jid}

	returnedJIDBytes, err = json.Marshal(returnedJID)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	w.WriteHeader(statusCode)
	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(statusCode))

	if statusCode != http.StatusOK {
		w.Write([]byte("Some errors occurred while creating containers. Check Apptainer Sidecar's logs"))
	} else {
		w.Write(returnedJIDBytes)
	}
}

// runJobDirectly starts job.sh as a new process and returns the PID string.
// The process is placed in its own process group so killProcessGroup can
// terminate all children when the pod is deleted.
// stdout/stderr of the script is redirected to job.out in the working directory.
func runJobDirectly(h *SidecarHandler, spanCtx interface{ Done() <-chan struct{} }, data commonIL.RetrievedPodData, scriptPath string, filesPath string) (string, error) {
	jobOutPath := filesPath + "/job.out"
	jobOutFile, err := os.Create(jobOutPath)
	if err != nil {
		return "", fmt.Errorf("could not create job.out: %w", err)
	}

	cmd := exec.Command(h.Config.BashPath, scriptPath)
	cmd.Stdout = jobOutFile
	cmd.Stderr = jobOutFile
	// New process group so we can kill all descendants with killProcessGroup.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		jobOutFile.Close()
		return "", fmt.Errorf("could not start job process: %w", err)
	}

	pid := strconv.Itoa(cmd.Process.Pid)
	log.G(h.Ctx).Infof("Started job for pod %s with PID %s", data.Pod.UID, pid)

	// Wait for the process in a goroutine to avoid zombie processes and to
	// record the finish time.
	go func() {
		defer jobOutFile.Close()
		if err := cmd.Wait(); err != nil {
			log.G(h.Ctx).Infof("Job process for pod %s (PID %s) exited with error: %v", data.Pod.UID, pid, err)
		} else {
			log.G(h.Ctx).Infof("Job process for pod %s (PID %s) exited successfully", data.Pod.UID, pid)
		}

		// Record the finish time so LoadJIDs can restore it after a restart.
		finishedAt := time.Now()
		finishedAtStr := finishedAt.Format("2006-01-02 15:04:05.999999999 -0700 MST")
		if err := os.WriteFile(filesPath+"/FinishedAt.time", []byte(finishedAtStr), 0644); err != nil {
			log.G(h.Ctx).Warning("Could not write FinishedAt.time: ", err)
		}

		// Update the in-memory JID entry with the end time.
		uid := string(data.Pod.UID)
		if entry, ok := (*h.JIDs)[uid]; ok {
			entry.EndTime = finishedAt
		}
	}()

	return pid, nil
}
