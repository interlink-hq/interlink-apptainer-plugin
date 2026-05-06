package apptainer

// ApptainerConfig holds the whole configuration for the apptainer plugin.
// Unlike the SLURM plugin, there are no batch-scheduler paths; the plugin
// runs job.sh directly as a subprocess.
type ApptainerConfig struct {
	VKConfigPath              string   `yaml:"VKConfigPath"`
	Sidecarport               string   `yaml:"SidecarPort"`
	Socket                    string   `yaml:"Socket"`
	ExportPodData             bool     `yaml:"ExportPodData"`
	Commandprefix             string   `yaml:"CommandPrefix"`
	ImagePrefix               string   `yaml:"ImagePrefix"`
	DataRootFolder            string   `yaml:"DataRootFolder"`
	Namespace                 string   `yaml:"Namespace"`
	Tsocks                    bool     `yaml:"Tsocks"`
	Tsockspath                string   `yaml:"TsocksPath"`
	Tsockslogin               string   `yaml:"TsocksLoginNode"`
	BashPath                  string   `yaml:"BashPath"`
	VerboseLogging            bool     `yaml:"VerboseLogging"`
	ErrorsOnlyLogging         bool     `yaml:"ErrorsOnlyLogging"`
	ApptainerPath             string   `yaml:"ApptainerPath"`
	ApptainerDefaultOptions   []string `yaml:"ApptainerDefaultOptions"`
	ApptainerPrefix           string   `yaml:"ApptainerPrefix"`
	EnableProbes              bool     `yaml:"EnableProbes"`
	set                       bool
}

// CreateStruct is returned to the interlink VK after a successful job submission.
type CreateStruct struct {
	PodUID string `json:"PodUID"`
	PodJID string `json:"PodJID"`
}

// ProbeType identifies whether a probe uses HTTP or exec.
type ProbeType string

const (
	ProbeTypeHTTP ProbeType = "http"
	ProbeTypeExec ProbeType = "exec"
)

// ProbeCommand is the runtime-agnostic representation of a single Kubernetes probe.
type ProbeCommand struct {
	Type                ProbeType
	HTTPGetAction       *HTTPGetAction
	ExecAction          *ExecAction
	InitialDelaySeconds int32
	PeriodSeconds       int32
	TimeoutSeconds      int32
	SuccessThreshold    int32
	FailureThreshold    int32
}

// HTTPGetAction describes an HTTP-based probe.
type HTTPGetAction struct {
	Path   string
	Port   int32
	Host   string
	Scheme string
}

// ExecAction describes an exec-based probe.
type ExecAction struct {
	Command []string
}

// LifecycleHookType indicates whether a lifecycle hook is an exec or httpGet hook.
type LifecycleHookType string

const (
	LifecycleHookTypeExec    LifecycleHookType = "exec"
	LifecycleHookTypeHTTPGet LifecycleHookType = "httpGet"
)

// LifecycleHTTPGetSpec holds the parameters for an httpGet-type lifecycle hook.
type LifecycleHTTPGetSpec struct {
	Scheme string
	Host   string
	Port   int32
	Path   string
}

// LifecycleHookSpec describes a container lifecycle hook (postStart or preStop)
// in a runtime-agnostic form.
type LifecycleHookSpec struct {
	Type        LifecycleHookType
	ExecCommand []string              // populated when Type == LifecycleHookTypeExec
	HTTPGet     *LifecycleHTTPGetSpec // populated when Type == LifecycleHookTypeHTTPGet
}

// ContainerCommand bundles all runtime-command information for a single container.
type ContainerCommand struct {
	containerName    string
	isInitContainer  bool
	runtimeCommand   []string
	containerCommand []string
	containerArgs    []string
	containerImage   string
	readinessProbes  []ProbeCommand
	livenessProbes   []ProbeCommand
	startupProbes    []ProbeCommand
	preStopHook      *LifecycleHookSpec
	postStartHook    *LifecycleHookSpec
}
