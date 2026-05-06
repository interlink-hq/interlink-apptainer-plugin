package apptainer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// GetLogsFollowMode streams container logs until the container exits.
func (h *SidecarHandler) GetLogsFollowMode(
	spanCtx context.Context,
	podUid string,
	w http.ResponseWriter,
	r *http.Request,
	path string,
	req commonIL.LogStruct,
	containerOutputPath string,
	containerOutput []byte,
	sessionContext string,
) error {
	containerStatusPath := path + "/run-" + req.ContainerName + ".status"
	containerOutputLastOffset := len(containerOutput)
	sessionContextMessage := GetSessionContextMessage(sessionContext)
	log.G(h.Ctx).Debug(sessionContextMessage, "Check container status", containerStatusPath, " with current length/offset: ", containerOutputLastOffset)

	var containerOutputFd *os.File
	var err error
	for {
		containerOutputFd, err = os.Open(containerOutputPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				notFoundMsg := sessionContextMessage + "Cannot open in follow mode the container logs " + containerOutputPath + " because it does not exist yet, sleeping before retrying..."
				log.G(h.Ctx).Debug(notFoundMsg)
				w.Write([]byte(notFoundMsg))
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				time.Sleep(4 * time.Second)
				continue
			} else {
				errWithContext := fmt.Errorf(sessionContextMessage+"could not open file to follow logs at %s error type: %s error: %w", containerOutputPath, fmt.Sprintf("%#v", err), err)
				log.G(h.Ctx).Error(errWithContext)
				w.Write([]byte(errWithContext.Error()))
				return err
			}
		}
		log.G(h.Ctx).Debug(sessionContextMessage, "opened for follow mode the container logs ", containerOutputPath)
		break
	}
	defer containerOutputFd.Close()

	_, err = containerOutputFd.Seek(int64(containerOutputLastOffset), 0)
	if err != nil {
		errWithContext := fmt.Errorf(sessionContextMessage+"error during Seek() of GetLogsFollowMode() in GetLogsHandler of file %s offset %d type: %s %w", containerOutputPath, containerOutputLastOffset, fmt.Sprintf("%#v", err), err)
		w.Write([]byte(errWithContext.Error()))
		return errWithContext
	}

	containerOutputReader := bufio.NewReader(containerOutputFd)
	bufferBytes := make([]byte, 4096)

	var isContainerDead bool = false
	for {
		n, errRead := containerOutputReader.Read(bufferBytes)
		if errRead != nil && errRead != io.EOF {
			h.logErrorVerbose(sessionContextMessage+"error doing Read() of GetLogsFollowMode", h.Ctx, w, errRead)
			return errRead
		}
		_, err = w.Write(bufferBytes[:n])
		if err != nil {
			h.logErrorVerbose(sessionContextMessage+"error doing Write() of GetLogsFollowMode", h.Ctx, w, err)
			return err
		}

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		if errRead != nil {
			if errRead == io.EOF {
				if isContainerDead {
					log.G(h.Ctx).Info(sessionContextMessage, "Container was found dead and no more logs are found at this step, exiting following mode...")
					break
				}
				if !checkIfJidExists(spanCtx, h.JIDs, podUid) {
					isContainerDead = true
					log.G(h.Ctx).Info(sessionContextMessage, "Container is found dead thanks to missing JID, reading last logs...")
				} else if _, err := os.Stat(containerStatusPath); errors.Is(err, os.ErrNotExist) {
					log.G(h.Ctx).Debug(sessionContextMessage, "EOF of container logs, sleeping 4s before retrying...")
					time.Sleep(4 * time.Second)
				} else {
					isContainerDead = true
					log.G(h.Ctx).Info(sessionContextMessage, "Container is found dead thanks to status file, reading last logs...")
				}
				continue
			}
		}
	}
	return nil
}

// GetLogsHandler reads the job output files and returns their content to the
// interlink VK based on the requested options (Tail/LimitBytes/Timestamps/Follow).
func (h *SidecarHandler) GetLogsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "GetLogs", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	sessionContext := GetSessionContext(r)
	sessionContextMessage := GetSessionContextMessage(sessionContext)

	log.G(h.Ctx).Info(sessionContextMessage, "Apptainer Sidecar: received GetLogs call")
	var req commonIL.LogStruct
	currentTime := time.Now()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		h.logErrorVerbose(sessionContextMessage+"error during ReadAll() in GetLogsHandler request body", spanCtx, w, err)
		return
	}

	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		h.logErrorVerbose(sessionContextMessage+"error during Unmarshal() in GetLogsHandler request body", spanCtx, w, err)
		return
	}

	span.SetAttributes(
		attribute.String("pod.name", req.PodName),
		attribute.String("pod.namespace", req.Namespace),
		attribute.Int("opts.limitbytes", req.Opts.LimitBytes),
		attribute.Int("opts.since", req.Opts.SinceSeconds),
		attribute.Int64("opts.sincetime", req.Opts.SinceTime.UnixMicro()),
		attribute.Int("opts.tail", req.Opts.Tail),
		attribute.Bool("opts.follow", req.Opts.Follow),
		attribute.Bool("opts.previous", req.Opts.Previous),
		attribute.Bool("opts.timestamps", req.Opts.Timestamps),
	)

	path := h.Config.DataRootFolder + req.Namespace + "-" + req.PodUID
	containerOutputPath := path + "/run-" + req.ContainerName + ".out"
	var output []byte

	if req.Opts.Timestamps {
		log.G(h.Ctx).Warning(sessionContextMessage, "unsupported option req.Opts.Timestamps, ignoring it")
	}

	containerOutput, err := h.ReadLogs(containerOutputPath, span, spanCtx, w, sessionContextMessage)
	if err != nil {
		log.G(h.Ctx).Warning(sessionContextMessage, "cannot find any container with this name, falling back to init containers")
		containerOutputPath = path + "/init-" + req.ContainerName + ".out"
		containerOutput, err = h.ReadLogs(containerOutputPath, span, spanCtx, w, sessionContextMessage)
		if err != nil {
			log.G(h.Ctx).Warning(sessionContextMessage, "cannot find any log for this container")
			return
		}
	}

	jobOutput, err := h.ReadLogs(path+"/"+"job.out", span, spanCtx, w, sessionContextMessage)
	if err != nil {
		return
	}

	output = append(output, jobOutput...)
	output = append(output, containerOutput...)

	var returnedLogs string

	if req.Opts.Tail != 0 {
		var lastLines []string
		splittedLines := strings.Split(string(output), "\n")
		if req.Opts.Tail > len(splittedLines) {
			lastLines = splittedLines
		} else {
			lastLines = splittedLines[len(splittedLines)-req.Opts.Tail-1:]
		}
		for _, line := range lastLines {
			returnedLogs += line + "\n"
		}
	} else if req.Opts.LimitBytes != 0 {
		var lastBytes []byte
		if req.Opts.LimitBytes > len(output) {
			lastBytes = output
		} else {
			lastBytes = output[len(output)-req.Opts.LimitBytes-1:]
		}
		returnedLogs = string(lastBytes)
	} else {
		returnedLogs = string(output)
	}

	if req.Opts.Timestamps && (req.Opts.SinceSeconds != 0 || !req.Opts.SinceTime.IsZero()) {
		temp := returnedLogs
		returnedLogs = ""
		splittedLogs := strings.Split(temp, "\n")
		timestampFormat := "2006-01-02T15:04:05.999999999Z"

		for _, Log := range splittedLogs {
			part := strings.SplitN(Log, " ", 2)
			timestampString := part[0]
			timestamp, err := time.Parse(timestampFormat, timestampString)
			if err != nil {
				continue
			}
			if req.Opts.SinceSeconds != 0 {
				if currentTime.Sub(timestamp).Seconds() > float64(req.Opts.SinceSeconds) {
					returnedLogs += Log + "\n"
				}
			} else {
				if timestamp.Sub(req.Opts.SinceTime).Seconds() >= 0 {
					returnedLogs += Log + "\n"
				}
			}
		}
	}

	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(http.StatusOK))

	w.Header().Set("Content-Type", "text/plain")
	log.G(h.Ctx).Info(sessionContextMessage, "writing response headers and OK status")
	w.WriteHeader(http.StatusOK)

	log.G(h.Ctx).Info(sessionContextMessage, "writing response body len: ", len(returnedLogs))
	n, err := w.Write([]byte(returnedLogs))
	log.G(h.Ctx).Info(sessionContextMessage, "written response body len: ", n)
	if err != nil {
		h.logErrorVerbose(sessionContextMessage+"error during Write() in GetLogsHandler, could write bytes: "+strconv.Itoa(n), spanCtx, w, err)
		return
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	if req.Opts.Follow {
		err := h.GetLogsFollowMode(spanCtx, req.PodUID, w, r, path, req, containerOutputPath, containerOutput, sessionContext)
		if err != nil {
			h.logErrorVerbose(sessionContextMessage+"follow mode error", spanCtx, w, err)
		}
	}
}

// ReadLogs reads a log file if it exists; returns an empty slice if the file is
// not found (so the caller can continue without error).
func (h *SidecarHandler) ReadLogs(logsPath string, span trace.Span, ctx context.Context, w http.ResponseWriter, sessionContextMessage string) ([]byte, error) {
	var output []byte
	var err error
	log.G(h.Ctx).Info(sessionContextMessage, "reading file ", logsPath)
	output, err = os.ReadFile(logsPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.G(h.Ctx).Info(sessionContextMessage, "file ", logsPath, " not found.")
			output = make([]byte, 0)
		} else {
			span.AddEvent("Error retrieving logs")
			h.logErrorVerbose(sessionContextMessage+"error during ReadFile() of readLogs() in GetLogsHandler of file "+logsPath, ctx, w, err)
			return nil, err
		}
	}
	return output, nil
}
