package apptainer

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// StatusHandler determines pod/container status by checking whether the job
// process (identified by the stored PID) is still alive, then reading the
// per-container .status files that job.sh writes upon completion.
//
// This replaces the SLURM-plugin approach of polling squeue: instead of a
// scheduler queue, we rely on Linux process existence and filesystem artifacts.
func (h *SidecarHandler) StatusHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "Status", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	sessionContext := GetSessionContext(r)
	sessionContextMessage := GetSessionContextMessage(sessionContext)

	var req []*v1.Pod
	var resp []commonIL.PodStatus
	statusCode := http.StatusOK
	log.G(h.Ctx).Info("Apptainer Sidecar: received GetStatus call")
	timeNow := time.Now()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	// When the request contains no pods, return a simple health summary.
	if len(req) == 0 {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Apptainer plugin running; no pods to report."))
		return
	}

	for _, pod := range req {
		containerStatuses := []v1.ContainerStatus{}
		uid := string(pod.UID)
		path := h.Config.DataRootFolder + pod.Namespace + "-" + string(pod.UID)

		if !checkIfJidExists(spanCtx, h.JIDs, uid) {
			// No JID registered — report all containers as Waiting so the VK
			// can decide what to do (e.g. re-submit).
			for _, ct := range pod.Spec.Containers {
				containerStatuses = append(containerStatuses, v1.ContainerStatus{
					Name: ct.Name,
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{Reason: "PodNotFound"},
					},
					Ready: false,
				})
			}
			resp = append(resp, commonIL.PodStatus{
				PodName:      pod.Name,
				PodUID:       uid,
				PodNamespace: pod.Namespace,
				Containers:   containerStatuses,
			})
			continue
		}

		jidStr := strings.TrimSpace((*h.JIDs)[uid].JID)
		pid, err := strconv.Atoi(jidStr)
		if err != nil {
			log.G(h.Ctx).Error(sessionContextMessage, "Could not parse PID from JID: ", jidStr)
			// Treat as dead process.
			pid = -1
		}

		processAlive := pid > 0 && isProcessAlive(pid)
		log.G(h.Ctx).Infof("%sPID: %s | Alive: %v | Pod: %s | UID: %s",
			sessionContextMessage, jidStr, processAlive, pod.Name, uid)

		if processAlive {
			// Record start time on first live check.
			if (*h.JIDs)[uid].StartTime.IsZero() {
				(*h.JIDs)[uid].StartTime = timeNow
				f, err := os.Create(path + "/StartedAt.time")
				if err != nil {
					statusCode = http.StatusInternalServerError
					h.handleError(spanCtx, w, statusCode, err)
					return
				}
				f.WriteString((*h.JIDs)[uid].StartTime.Format("2006-01-02 15:04:05.999999999 -0700 MST"))
				f.Close()
			}

			for _, ct := range pod.Spec.Containers {
				statusFile := path + "/run-" + ct.Name + ".status"
				if _, statErr := os.Stat(statusFile); statErr == nil {
					// Container has already finished — read its exit code.
					exitCode, err := getExitCode(h.Ctx, path, ct.Name, "0", sessionContextMessage)
					if err != nil {
						log.G(h.Ctx).Error(sessionContextMessage, err)
						exitCode = 1
					}
					containerStatuses = append(containerStatuses, v1.ContainerStatus{
						Name: ct.Name,
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{
								StartedAt:  metav1.Time{Time: (*h.JIDs)[uid].StartTime},
								FinishedAt: metav1.Time{Time: timeNow},
								ExitCode:   exitCode,
							},
						},
						Ready: false,
					})
				} else {
					// Container is still running — check probe readiness.
					isReady := true
					if h.Config.EnableProbes {
						readinessCount, livenessCount, startupCount, err := loadProbeMetadata(path, ct.Name)
						if err == nil {
							startupComplete := checkContainerStartupComplete(spanCtx, h.Config, path, ct.Name, startupCount)
							readinessOK := checkContainerReadiness(spanCtx, h.Config, path, ct.Name, readinessCount)
							livenessOK := checkContainerLiveness(spanCtx, h.Config, path, ct.Name, livenessCount)
							isReady = startupComplete && readinessOK && livenessOK
						}
					}

					if isReady {
						containerStatuses = append(containerStatuses, v1.ContainerStatus{
							Name: ct.Name,
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{
									StartedAt: metav1.Time{Time: (*h.JIDs)[uid].StartTime},
								},
							},
							Ready: true,
						})
					} else {
						containerStatuses = append(containerStatuses, v1.ContainerStatus{
							Name: ct.Name,
							State: v1.ContainerState{
								Waiting: &v1.ContainerStateWaiting{
									Reason: "Waiting for probes to be ready.",
								},
							},
							Ready: false,
						})
					}
				}
			}
		} else {
			// Process has exited — record end time and report all containers as
			// Terminated, reading exit codes from the .status files.
			if (*h.JIDs)[uid].EndTime.IsZero() {
				(*h.JIDs)[uid].EndTime = timeNow
				f, err := os.Create(path + "/FinishedAt.time")
				if err != nil {
					// Non-fatal: log and continue.
					log.G(h.Ctx).Warning("Could not create FinishedAt.time: ", err)
				} else {
					f.WriteString((*h.JIDs)[uid].EndTime.Format("2006-01-02 15:04:05.999999999 -0700 MST"))
					f.Close()
				}
			}

			for _, ct := range pod.Spec.Containers {
				exitCode, err := getExitCode(h.Ctx, path, ct.Name, "1", sessionContextMessage)
				if err != nil {
					log.G(h.Ctx).Error(sessionContextMessage, err)
					exitCode = 1
				}
				containerStatuses = append(containerStatuses, v1.ContainerStatus{
					Name: ct.Name,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							StartedAt:  metav1.Time{Time: (*h.JIDs)[uid].StartTime},
							FinishedAt: metav1.Time{Time: (*h.JIDs)[uid].EndTime},
							ExitCode:   exitCode,
						},
					},
					Ready: false,
				})
			}
		}

		span.SetAttributes(
			attribute.String("status.pod.uid", uid),
			attribute.String("status.pid", jidStr),
			attribute.Bool("status.process.alive", processAlive),
		)

		resp = append(resp, commonIL.PodStatus{
			PodName:      pod.Name,
			PodUID:       uid,
			PodNamespace: pod.Namespace,
			Containers:   containerStatuses,
		})
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.handleError(spanCtx, w, statusCode, err)
		return
	}

	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(statusCode))
	w.WriteHeader(statusCode)
	w.Write(respBytes)
}
