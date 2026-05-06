package apptainer

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"go.opentelemetry.io/otel/trace"

	"github.com/containerd/containerd/log"
	"github.com/goccy/go-yaml"
)

var ApptainerConfigInst ApptainerConfig

// NewApptainerConfig parses command-line flags and the YAML config file, then
// returns a populated ApptainerConfig.  Subsequent calls return the cached
// value without re-parsing.
func NewApptainerConfig() (ApptainerConfig, error) {
	if !ApptainerConfigInst.set {
		var path string
		verbose := flag.Bool("verbose", false, "Enable or disable Debug level logging")
		errorsOnly := flag.Bool("errorsonly", false, "Prints only errors if enabled")
		configPath := flag.String("ApptainerConfigpath", "", "Path to Apptainer plugin config")
		flag.Parse()

		if *verbose {
			ApptainerConfigInst.VerboseLogging = true
			ApptainerConfigInst.ErrorsOnlyLogging = false
		} else if *errorsOnly {
			ApptainerConfigInst.VerboseLogging = false
			ApptainerConfigInst.ErrorsOnlyLogging = true
		}

		if *configPath != "" {
			path = *configPath
		} else if os.Getenv("APPTAINERCONFIGPATH") != "" {
			path = os.Getenv("APPTAINERCONFIGPATH")
		} else {
			path = "/etc/interlink/ApptainerConfig.yaml"
		}

		if _, err := os.Stat(path); err != nil {
			log.G(context.Background()).Error("File " + path + " doesn't exist. You can set a custom path by exporting APPTAINERCONFIGPATH. Exiting...")
			return ApptainerConfig{}, err
		}

		log.G(context.Background()).Info("Loading Apptainer config from " + path)
		yfile, err := os.ReadFile(path)
		if err != nil {
			log.G(context.Background()).Error("Error opening config file, exiting...")
			return ApptainerConfig{}, err
		}
		yaml.Unmarshal(yfile, &ApptainerConfigInst)

		if os.Getenv("SIDECARPORT") != "" {
			ApptainerConfigInst.Sidecarport = os.Getenv("SIDECARPORT")
		}

		if os.Getenv("APPTAINERPATH") != "" {
			ApptainerConfigInst.ApptainerPath = os.Getenv("APPTAINERPATH")
		}

		if os.Getenv("TSOCKS") != "" {
			if os.Getenv("TSOCKS") != "true" && os.Getenv("TSOCKS") != "false" {
				fmt.Println("export TSOCKS as true or false")
				return ApptainerConfig{}, fmt.Errorf("invalid TSOCKS value: must be 'true' or 'false'")
			}
			ApptainerConfigInst.Tsocks = os.Getenv("TSOCKS") == "true"
		}

		if os.Getenv("TSOCKSPATH") != "" {
			tsocksPath := os.Getenv("TSOCKSPATH")
			if _, err := os.Stat(tsocksPath); err != nil {
				log.G(context.Background()).Error("File " + tsocksPath + " doesn't exist. You can set a custom path by exporting TSOCKSPATH. Exiting...")
				return ApptainerConfig{}, err
			}
			ApptainerConfigInst.Tsockspath = tsocksPath
		}

		// Set defaults
		if ApptainerConfigInst.ApptainerPath == "" {
			ApptainerConfigInst.ApptainerPath = "apptainer"
		}

		if ApptainerConfigInst.BashPath == "" {
			ApptainerConfigInst.BashPath = "/bin/bash"
		}

		if len(ApptainerConfigInst.ApptainerDefaultOptions) == 0 {
			ApptainerConfigInst.ApptainerDefaultOptions = []string{"--nv", "--no-eval", "--containall"}
		}

		ApptainerConfigInst.set = true
	}
	return ApptainerConfigInst, nil
}

func (h *SidecarHandler) handleError(ctx context.Context, w http.ResponseWriter, statusCode int, err error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("An error occurred:" + err.Error())
	w.WriteHeader(statusCode)
	w.Write([]byte(err.Error()))
	log.G(h.Ctx).Error(err)
}

func (h *SidecarHandler) logErrorVerbose(errContext string, ctx context.Context, w http.ResponseWriter, err error) {
	errWithContext := fmt.Errorf("error context: %s type: %s %w", errContext, fmt.Sprintf("%#v", err), err)
	log.G(h.Ctx).Error(errWithContext)
	h.handleError(ctx, w, http.StatusInternalServerError, errWithContext)
}

func GetSessionContext(r *http.Request) string {
	sessionContext := r.Header.Get("InterLink-Http-Session")
	if sessionContext == "" {
		sessionContext = "NoSessionFound#0"
	}
	return sessionContext
}

func GetSessionContextMessage(sessionContext string) string {
	return "HTTP InterLink session " + sessionContext + ": "
}
