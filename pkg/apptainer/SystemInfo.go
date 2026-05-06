package apptainer

import (
	"encoding/json"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/containerd/containerd/log"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// SystemInfoResponse is the response body for the /system-info endpoint.
type SystemInfoResponse struct {
	Status            string `json:"status"`
	Timestamp         string `json:"timestamp"`
	ApptainerVersion  string `json:"apptainer_version,omitempty"`
	Error             string `json:"error,omitempty"`
}

// SystemInfoHandler returns basic health information including the apptainer
// version so the interlink VK can verify that the plugin is functional.
func (h *SidecarHandler) SystemInfoHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	_, span := tracer.Start(h.Ctx, "SystemInfo", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	log.G(h.Ctx).Info("Apptainer Sidecar: received SystemInfo call")

	response := SystemInfoResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	// Query the apptainer version to verify the binary is reachable.
	version, err := getApptainerVersion(h.Config.ApptainerPath)
	if err != nil {
		log.G(h.Ctx).Warning("Failed to get apptainer version: ", err)
		response.Error = err.Error()
		response.Status = "warning"
	} else {
		response.ApptainerVersion = version
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	responseBytes, err := json.Marshal(response)
	if err != nil {
		log.G(h.Ctx).Error("Failed to marshal system info response: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"status":"error","error":"failed to marshal response"}`))
		return
	}

	w.Write(responseBytes)
	log.G(h.Ctx).Info("SystemInfo response sent successfully")
}

// getApptainerVersion runs `apptainer version` and returns the trimmed output.
func getApptainerVersion(apptainerPath string) (string, error) {
	cmd := exec.Command(apptainerPath, "version")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
