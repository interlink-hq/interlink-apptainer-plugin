# interlink-apptainer-plugin

An [interLink](https://github.com/interlink-hq/interlink) sidecar plugin that runs
container workloads directly via [Apptainer](https://apptainer.org/) — **without** a
batch-scheduler layer.

Compared to the [interlink-slurm-plugin](https://github.com/interlink-hq/interlink-slurm-plugin),
this plugin skips SLURM batch-job submission (`sbatch`/`squeue`/`scancel`) and instead
executes `job.sh` directly as an OS subprocess.  This is designed for the use-case where
the plugin process is already running inside a job (one job ↔ one virtual node), so no
additional scheduling step is needed.

## How it works

1. The interlink Virtual Kubelet sends a **Create** request to the plugin.
2. The plugin generates `job.sh` — a shell script that runs each container image via
   `apptainer run` / `apptainer exec` (identical logic to the SLURM plugin).
3. `job.sh` is executed **directly** as a child process (no sbatch).  The process PID is
   used as the "job ID".
4. Container status is determined by:
   - Checking whether the child process is still alive (`kill -0 <pid>`).
   - Reading per-container `.status` files that `job.sh` writes upon exit.
5. Deleting a pod sends `SIGTERM` to the entire process group.

## Configuration

The plugin reads a YAML configuration file.  The default path is
`/etc/interlink/ApptainerConfig.yaml`; override via the `APPTAINERCONFIGPATH`
environment variable or the `-ApptainerConfigpath` flag.

```yaml
# Port to listen on (when not using a Unix socket)
SidecarPort: "4000"

# Optional Unix socket path (e.g. "unix:///tmp/apptainer.sock")
# Socket: ""

# Directory where per-pod working data is stored
DataRootFolder: "/tmp/.interlink/"

# Path to the apptainer binary
ApptainerPath: "apptainer"

# Default options passed to every apptainer run/exec call
ApptainerDefaultOptions:
  - "--nv"
  - "--no-eval"
  - "--containall"

# Prefix added to image names that have no URI scheme
# ImagePrefix: "docker://"

# Path to bash used to execute job.sh
BashPath: "/bin/bash"

# Whether to export pod data (ConfigMaps, Secrets) to the working directory
ExportPodData: true

# Verbose / errors-only logging
VerboseLogging: false
ErrorsOnlyLogging: false

# Enable Kubernetes probe support
EnableProbes: false
```

### Environment variable overrides

| Variable              | Config field      |
|-----------------------|-------------------|
| `APPTAINERCONFIGPATH` | config file path  |
| `SIDECARPORT`         | `SidecarPort`     |
| `APPTAINERPATH`       | `ApptainerPath`   |
| `TSOCKS`              | `Tsocks`          |
| `TSOCKSPATH`          | `TsocksPath`      |

## Building

```bash
go build -o bin/apptainer-plugin ./cmd
```

## Running

```bash
APPTAINERCONFIGPATH=/etc/interlink/ApptainerConfig.yaml ./bin/apptainer-plugin
```

## Differences from the SLURM plugin

| Feature                        | SLURM plugin          | Apptainer plugin      |
|--------------------------------|-----------------------|-----------------------|
| Job submission                 | `sbatch`              | Direct `exec`         |
| Status polling                 | `squeue`              | `kill -0 <pid>` + `.status` files |
| Job cancellation               | `scancel`             | `SIGTERM` to process group |
| Cluster info endpoint          | `sinfo -s`            | `apptainer version`   |
| Flavors / SLURM flags          | Yes                   | No                    |
| Container runtime              | singularity / enroot  | apptainer             |
