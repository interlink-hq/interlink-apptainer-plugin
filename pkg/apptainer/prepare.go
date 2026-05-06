package apptainer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"al.essio.dev/pkg/shellescape"
	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// SidecarHandler holds the shared state needed by all HTTP handler methods.
type SidecarHandler struct {
	Config ApptainerConfig
	JIDs   *map[string]*JidStruct
	Ctx    context.Context
}

var (
	prefix       string
	cachedStatus []commonIL.PodStatus
)

// JidStruct stores the runtime information for a submitted job.
// In the apptainer plugin the JID field holds the OS process PID (as a string).
type JidStruct struct {
	PodUID       string    `json:"PodUID"`
	PodNamespace string    `json:"PodNamespace"`
	JID          string    `json:"JID"` // OS process PID
	StartTime    time.Time `json:"StartTime"`
	EndTime      time.Time `json:"EndTime"`
}

// ResourceLimits carries the resolved CPU and memory limits for a pod.
type ResourceLimits struct {
	CPU    int64
	Memory int64
}

func extractHeredoc(content, marker string) (string, error) {
	startPattern := fmt.Sprintf("cat <<'%s'", marker)
	startIdx := strings.Index(content, startPattern)
	if startIdx == -1 {
		return "", fmt.Errorf("heredoc start marker not found")
	}
	contentStart := strings.Index(content[startIdx:], "\n")
	if contentStart == -1 {
		return "", fmt.Errorf("invalid heredoc format")
	}
	contentStart += startIdx + 1
	endMarker := "\n" + marker
	endIdx := strings.Index(content[contentStart:], endMarker)
	if endIdx == -1 {
		return "", fmt.Errorf("heredoc end marker not found")
	}
	return content[contentStart : contentStart+endIdx], nil
}

func removeHeredoc(content, marker string) string {
	startPattern := fmt.Sprintf("cat <<'%s'", marker)
	startIdx := strings.Index(content, startPattern)
	if startIdx == -1 {
		return content
	}
	contentStart := strings.Index(content[startIdx:], "\n")
	if contentStart == -1 {
		return content
	}
	contentStart += startIdx + 1
	endMarker := "\n" + marker
	endIdx := strings.Index(content[contentStart:], endMarker)
	if endIdx == -1 {
		return content
	}
	heredocEnd := contentStart + endIdx + len(endMarker)
	if heredocEnd < len(content) && content[heredocEnd] == '\n' {
		heredocEnd++
	}
	return content[:startIdx] + content[heredocEnd:]
}

// stringToHex encodes str into a compact hex string (trailing zero pairs stripped).
func stringToHex(str string) string {
	var buffer bytes.Buffer
	for _, char := range str {
		err := binary.Write(&buffer, binary.LittleEndian, char)
		if err != nil {
			fmt.Println("Error converting character:", err)
			return ""
		}
	}
	hexString := hex.EncodeToString(buffer.Bytes())
	hexBytes := []byte(hexString)
	var hexReturn string
	for i := 0; i < len(hexBytes); i += 2 {
		if hexBytes[i] != 48 && hexBytes[i+1] != 48 {
			hexReturn += string(hexBytes[i]) + string(hexBytes[i+1])
		}
	}
	return hexReturn
}

func parsingTimeFromString(Ctx context.Context, stringTime string, timestampFormat string) (time.Time, error) {
	parts := strings.Fields(stringTime)
	if len(parts) != 4 {
		err := errors.New("invalid timestamp format")
		log.G(Ctx).Error(err)
		return time.Time{}, err
	}
	parsedTime, err := time.Parse(timestampFormat, stringTime)
	if err != nil {
		log.G(Ctx).Error(err)
		return time.Time{}, err
	}
	return parsedTime, nil
}

// normalizeVolumeFileContent converts literal \n sequences to real newlines when
// no real newlines are already present (handles a common YAML misconfiguration).
func normalizeVolumeFileContent(s string) []byte {
	if !strings.Contains(s, `\n`) || strings.ContainsRune(s, '\n') {
		return []byte(s)
	}
	return []byte(strings.ReplaceAll(s, `\n`, "\n"))
}

// CreateDirectories ensures the DataRootFolder exists at runtime.
func (h *SidecarHandler) CreateDirectories() error {
	path := h.Config.DataRootFolder
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadJIDs restores the in-memory JIDs map from files in DataRootFolder.
// This is useful when the plugin is restarted while jobs are still running.
func (h *SidecarHandler) LoadJIDs() error {
	path := h.Config.DataRootFolder

	dir, err := os.Open(path)
	if err != nil {
		log.G(h.Ctx).Error(err)
		return err
	}
	defer dir.Close()

	entries, err := dir.ReadDir(0)
	if err != nil {
		log.G(h.Ctx).Error(err)
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			var podNamespace []byte
			var podUID []byte
			StartedAt := time.Time{}
			FinishedAt := time.Time{}

			JID, err := os.ReadFile(path + entry.Name() + "/" + "JobID.jid")
			if err != nil {
				log.G(h.Ctx).Debug(err)
				continue
			} else {
				podUID, err = os.ReadFile(path + entry.Name() + "/" + "PodUID.uid")
				if err != nil {
					log.G(h.Ctx).Debug(err)
					continue
				} else {
					podNamespace, err = os.ReadFile(path + entry.Name() + "/" + "PodNamespace.ns")
					if err != nil {
						log.G(h.Ctx).Debug(err)
						continue
					}
				}

				StartedAtString, err := os.ReadFile(path + entry.Name() + "/" + "StartedAt.time")
				if err != nil {
					log.G(h.Ctx).Debug(err)
				} else {
					StartedAt, err = parsingTimeFromString(h.Ctx, string(StartedAtString), "2006-01-02 15:04:05.999999999 -0700 MST")
					if err != nil {
						log.G(h.Ctx).Debug(err)
					}
				}
			}

			FinishedAtString, err := os.ReadFile(path + entry.Name() + "/" + "FinishedAt.time")
			if err != nil {
				log.G(h.Ctx).Debug(err)
			} else {
				FinishedAt, err = parsingTimeFromString(h.Ctx, string(FinishedAtString), "2006-01-02 15:04:05.999999999 -0700 MST")
				if err != nil {
					log.G(h.Ctx).Debug(err)
				}
			}
			JIDEntry := JidStruct{PodUID: string(podUID), PodNamespace: string(podNamespace), JID: string(JID), StartTime: StartedAt, EndTime: FinishedAt}
			(*h.JIDs)[string(podUID)] = &JIDEntry
		}
	}

	return nil
}

func createEnvFile(Ctx context.Context, config ApptainerConfig, podData commonIL.RetrievedPodData, container v1.Container) ([]string, []string, error) {
	envs := []string{}
	envs_data := []string{}

	envfilePath := (config.DataRootFolder + podData.Pod.Namespace + "-" + string(podData.Pod.UID) + "/" + container.Name + "_envfile.properties")
	log.G(Ctx).Info("-- Appending envs using envfile " + envfilePath)

	envs = append(envs, "--env-file")
	envs = append(envs, envfilePath)

	envfile, err := os.Create(envfilePath)
	if err != nil {
		log.G(Ctx).Error(err)
		return nil, nil, err
	}
	defer envfile.Close()

	for _, envVar := range container.Env {
		tmpValue := shellescape.Quote(envVar.Value)
		tmp := (envVar.Name + "=" + tmpValue)
		envs_data = append(envs_data, tmp)
		_, err := envfile.WriteString(tmp + "\n")
		if err != nil {
			log.G(Ctx).Error(err)
			return nil, nil, err
		} else {
			log.G(Ctx).Debug("---- Written envfile file " + envfilePath + " key " + envVar.Name + " value " + tmpValue)
		}
	}

	err = envfile.Sync()
	if err != nil {
		log.G(Ctx).Error(err)
		return nil, nil, err
	}
	envfile.Close()

	return envs, envs_data, nil
}

// prepareEnvs reads environment variables from a container and writes them to an
// envfile.  Returns the slice of arguments to pass to apptainer (--env-file <path>).
func prepareEnvs(Ctx context.Context, config ApptainerConfig, podData commonIL.RetrievedPodData, container v1.Container) []string {
	start := time.Now().UnixMicro()
	span := trace.SpanFromContext(Ctx)
	span.AddEvent("Preparing ENVs for container " + container.Name)
	var envs []string = []string{}
	var envs_data []string = []string{}
	var err error

	if len(container.Env) > 0 {
		envs, envs_data, err = createEnvFile(Ctx, config, podData, container)
		if err != nil {
			log.G(Ctx).Error(err)
			return nil
		}
	}

	duration := time.Now().UnixMicro() - start
	span.AddEvent("Prepared ENVs for container "+container.Name, trace.WithAttributes(
		attribute.String("prepareenvs.container.name", container.Name),
		attribute.Int64("prepareenvs.duration", duration),
		attribute.StringSlice("prepareenvs.container.envs", envs),
		attribute.StringSlice("prepareenvs.container.envs_data", envs_data)))

	return envs
}

func getRetrievedContainer(podData *commonIL.RetrievedPodData, containerName string) (*commonIL.RetrievedContainer, error) {
	for _, container := range podData.Containers {
		if container.Name == containerName {
			return &container, nil
		}
	}
	return nil, fmt.Errorf("could not find retrieved container for %s in pod %s", containerName, podData.Pod.Name)
}

func getRetrievedConfigMap(retrievedContainer *commonIL.RetrievedContainer, configMapName string, containerName string, podName string) (*v1.ConfigMap, error) {
	for _, configMap := range retrievedContainer.ConfigMaps {
		if configMap.Name == configMapName {
			return &configMap, nil
		}
	}
	return nil, fmt.Errorf("could not find configMap %s in container %s in pod %s", configMapName, containerName, podName)
}

func getRetrievedProjectedVolumeMap(retrievedContainer *commonIL.RetrievedContainer, projectedVolumeMapName string, containerName string, podName string) (*v1.ConfigMap, error) {
	for _, retrievedProjectedVolumeMap := range retrievedContainer.ProjectedVolumeMaps {
		if retrievedProjectedVolumeMap.Name == projectedVolumeMapName {
			return &retrievedProjectedVolumeMap, nil
		}
	}
	return nil, nil
}

func getRetrievedSecret(retrievedContainer *commonIL.RetrievedContainer, secretName string, containerName string, podName string) (*v1.Secret, error) {
	for _, retrievedSecret := range retrievedContainer.Secrets {
		if retrievedSecret.Name == secretName {
			return &retrievedSecret, nil
		}
	}
	return nil, fmt.Errorf("could not find secret %s in container %s in pod %s", secretName, containerName, podName)
}

func getPodVolume(pod *v1.Pod, volumeName string) (*v1.Volume, error) {
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == volumeName {
			return &vol, nil
		}
	}
	return nil, fmt.Errorf("could not find volume %s in pod %s", volumeName, pod.Name)
}

func prepareMountsSimpleVolume(
	Ctx context.Context,
	config ApptainerConfig,
	container *v1.Container,
	workingPath string,
	volumeObject interface{},
	volumeMount v1.VolumeMount,
	volume v1.Volume,
	mountedDataSB *strings.Builder,
) error {
	volumesHostToContainerPaths, envVarNames, err := mountData(Ctx, config, container, volumeObject, volumeMount, volume, workingPath)
	if err != nil {
		log.G(Ctx).Error(err)
		return err
	}

	log.G(Ctx).Debug("volumesHostToContainerPaths: ", volumesHostToContainerPaths)

	for filePathIndex, volumesHostToContainerPath := range volumesHostToContainerPaths {
		if os.Getenv("SHARED_FS") != "true" {
			filePathSplitted := strings.Split(volumesHostToContainerPath, ":")
			hostFilePath := filePathSplitted[0]
			hostParentDir := filepath.Dir(hostFilePath)

			prefix += "\nmkdir -p \"" + hostParentDir + "\" && touch \"" + hostFilePath + "\""

			envVarName := envVarNames[filePathIndex]
			splittedEnvName := strings.Split(envVarName, "_")
			uniqueVolumeID := splittedEnvName[len(splittedEnvName)-1]
			log.G(Ctx).Info(uniqueVolumeID)
			content := os.Getenv(envVarName)
			b64Content := base64.StdEncoding.EncodeToString([]byte(content))
			heredocMarker := "VKDATA_" + uniqueVolumeID
			prefix += "\nbase64 -d <<'" + heredocMarker + "' > \"" + hostFilePath + "\"\n" + b64Content + "\n" + heredocMarker
		}
		mountedDataSB.WriteString(" --bind ")
		mountedDataSB.WriteString(volumesHostToContainerPath)
	}
	return nil
}

// prepareMounts iterates over volume mounts and produces the --bind arguments
// for apptainer.  Side-effects on the global `prefix` variable (file creation
// commands for non-shared-FS mode) are expected.
func prepareMounts(
	Ctx context.Context,
	config ApptainerConfig,
	podData *commonIL.RetrievedPodData,
	container *v1.Container,
	workingPath string,
) (string, error) {
	span := trace.SpanFromContext(Ctx)
	start := time.Now().UnixMicro()
	log.G(Ctx).Info(span)
	span.AddEvent("Preparing Mounts for container " + container.Name)

	log.G(Ctx).Info("-- Preparing mountpoints for ", container.Name)
	var mountedDataSB strings.Builder

	err := os.MkdirAll(workingPath, os.ModePerm)
	if err != nil {
		log.G(Ctx).Error(err)
		return "", err
	}
	log.G(Ctx).Info("-- Created directory ", workingPath)
	podName := podData.Pod.Name

	for _, volumeMount := range container.VolumeMounts {
		volumePtr, err := getPodVolume(&podData.Pod, volumeMount.Name)
		volume := *volumePtr
		if err != nil {
			return "", err
		}

		retrievedContainer, err := getRetrievedContainer(podData, container.Name)
		if err != nil {
			return "", err
		}

		switch {
		case volume.ConfigMap != nil:
			retrievedConfigMap, err := getRetrievedConfigMap(retrievedContainer, volume.ConfigMap.Name, container.Name, podName)
			if err != nil {
				return "", err
			}
			err = prepareMountsSimpleVolume(Ctx, config, container, workingPath, *retrievedConfigMap, volumeMount, volume, &mountedDataSB)
			if err != nil {
				return "", err
			}

		case volume.Projected != nil:
			retrievedProjectedVolumeMap, err := getRetrievedProjectedVolumeMap(retrievedContainer, volume.Name, container.Name, podName)
			if err != nil {
				return "", err
			}
			if retrievedProjectedVolumeMap == nil {
				var retrievedProjectedVolumeMapKeys []string
				for _, retrievedProjectedVolumeMap := range retrievedContainer.ProjectedVolumeMaps {
					retrievedProjectedVolumeMapKeys = append(retrievedProjectedVolumeMapKeys, retrievedProjectedVolumeMap.Name)
				}
				log.G(Ctx).Warningf("projected volumes not found %s in container %s in pod %s, current projectedVolumeMaps keys %s ."+
					"either this is an error or this is because InterLink VK has DisableProjectedVolumes set to true.",
					volume.Name, container.Name, podName, strings.Join(retrievedProjectedVolumeMapKeys, ","))
			} else {
				err = prepareMountsSimpleVolume(Ctx, config, container, workingPath, *retrievedProjectedVolumeMap, volumeMount, volume, &mountedDataSB)
				if err != nil {
					return "", err
				}
			}

		case volume.Secret != nil:
			retrievedSecret, err := getRetrievedSecret(retrievedContainer, volume.Secret.SecretName, container.Name, podName)
			if err != nil {
				return "", err
			}
			err = prepareMountsSimpleVolume(Ctx, config, container, workingPath, *retrievedSecret, volumeMount, volume, &mountedDataSB)
			if err != nil {
				return "", err
			}

		case volume.EmptyDir != nil:
			edPath, _, err := mountData(Ctx, config, container, "emptyDir", volumeMount, volume, workingPath)
			if err != nil {
				log.G(Ctx).Error(err)
				return "", err
			}
			log.G(Ctx).Debug("edPath: ", edPath)
			for _, mntData := range edPath {
				mountedDataSB.WriteString(mntData)
			}

		case volume.HostPath != nil:
			log.G(Ctx).Info("Handling hostPath volume: ", volume.Name)
			hostPath := volume.HostPath.Path
			containerPath := volumeMount.MountPath

			if hostPath == "" || containerPath == "" {
				err := fmt.Errorf("hostPath or containerPath is empty for volume %s in pod %s", volume.Name, podName)
				log.G(Ctx).Error(err)
				return "", err
			}

			if volume.Name != volumeMount.Name {
				log.G(Ctx).Warningf("Volume name %s does not match volumeMount name %s in pod %s", volume.Name, volumeMount.Name, podName)
				continue
			}

			if volume.HostPath.Type != nil && *volume.HostPath.Type == v1.HostPathDirectory {
				if _, err := os.Stat(hostPath); os.IsNotExist(err) {
					err := fmt.Errorf("hostPath directory %s does not exist for volume %s in pod %s", hostPath, volume.Name, podName)
					log.G(Ctx).Error(err)
					return "", err
				}
			} else if volume.HostPath.Type != nil && *volume.HostPath.Type == v1.HostPathDirectoryOrCreate {
				if _, err := os.Stat(hostPath); os.IsNotExist(err) {
					err = os.MkdirAll(hostPath, os.ModePerm)
					if err != nil {
						log.G(Ctx).Error(err)
						return "", err
					}
				}
			} else {
				err := fmt.Errorf("unsupported hostPath type for volume %s in pod %s", volume.Name, podName)
				log.G(Ctx).Error(err)
				return "", err
			}

			mountedDataSB.WriteString(" --bind ")
			mountedDataSB.WriteString(hostPath + ":" + containerPath)
			if volumeMount.ReadOnly {
				mountedDataSB.WriteString(":ro")
			}

		default:
			log.G(Ctx).Warningf("Silently ignoring unknown volume type of volume: %s in pod %s", volume.Name, podName)
			return "", nil
		}
	}

	mountedData := mountedDataSB.String()
	if last := len(mountedData) - 1; last >= 0 && mountedData[last] == ',' {
		mountedData = mountedData[:last]
	}
	if len(mountedData) == 0 {
		return "", nil
	}
	log.G(Ctx).Debug(mountedData)

	duration := time.Now().UnixMicro() - start
	span.AddEvent("Prepared mounts for container "+container.Name, trace.WithAttributes(
		attribute.String("peparemounts.container.name", container.Name),
		attribute.Int64("preparemounts.duration", duration),
		attribute.String("preparemounts.container.mounts", mountedData)))

	return mountedData, nil
}

// splitShellWords tokenises a shell-like input string, respecting single/double
// quotes and backslash escapes.
func splitShellWords(input string) []string {
	var (
		tokens       []string
		currentToken strings.Builder
		inSingle     bool
		inDouble     bool
		escaping     bool
		tokenStarted bool
	)

	flushToken := func() {
		if tokenStarted {
			tokens = append(tokens, currentToken.String())
			currentToken.Reset()
			tokenStarted = false
		}
	}

	for _, r := range input {
		switch {
		case escaping:
			currentToken.WriteRune(r)
			escaping = false
			tokenStarted = true
		case r == '\\' && !inSingle:
			escaping = true
			tokenStarted = true
		case r == '\'' && !inDouble:
			inSingle = !inSingle
			tokenStarted = true
		case r == '"' && !inSingle:
			inDouble = !inDouble
			tokenStarted = true
		case unicode.IsSpace(r) && !inSingle && !inDouble:
			flushToken()
		default:
			currentToken.WriteRune(r)
			tokenStarted = true
		}
	}

	if escaping {
		currentToken.WriteRune('\\')
	}
	flushToken()

	return tokens
}

// produceApptainerScript generates job.sh, which is then executed directly (no
// batch scheduler submission).  Unlike the SLURM plugin there is no job.slurm
// wrapper; the prefix (file-setup commands) and the container execution logic
// are merged into a single job.sh file.
//
// Returns the path to job.sh and the first encountered error.
func produceApptainerScript(
	Ctx context.Context,
	config ApptainerConfig,
	pod v1.Pod,
	path string,
	metadata metav1.ObjectMeta,
	commands []ContainerCommand,
) (string, error) {
	start := time.Now().UnixMicro()
	span := trace.SpanFromContext(Ctx)
	span.AddEvent("Producing Apptainer script")

	podUID := string(pod.UID)

	log.G(Ctx).Info("-- Creating file for the Apptainer script")
	prefix = ""
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.G(Ctx).Error(err)
		return "", err
	} else {
		log.G(Ctx).Info("-- Created directory " + path)
	}

	postfix := ""

	f, err := os.Create(path + "/job.sh")
	if err != nil {
		log.G(Ctx).Error("Unable to create file ", path, "/job.sh")
		log.G(Ctx).Error(err)
		return "", err
	}
	defer f.Close()

	err = os.Chmod(path+"/job.sh", 0774)
	if err != nil {
		log.G(Ctx).Error("Unable to chmod file ", path, "/job.sh")
		log.G(Ctx).Error(err)
		return "", err
	}

	if config.Tsocks {
		log.G(Ctx).Debug("--- Adding SSH connection and setting ENVs to use TSOCKS")
		postfix += "\n\nkill -15 $SSH_PID &> log2.txt"

		prefix += "\n\nmin_port=10000"
		prefix += "\nmax_port=65000"
		prefix += "\nfor ((port=$min_port; port<=$max_port; port++))"
		prefix += "\ndo"
		prefix += "\n  temp=$(ss -tulpn | grep :$port)"
		prefix += "\n  if [ -z \"$temp\" ]"
		prefix += "\n  then"
		prefix += "\n    break"
		prefix += "\n  fi"
		prefix += "\ndone"
		prefix += "\nssh -4 -N -D $port " + config.Tsockslogin + " &"
		prefix += "\nSSH_PID=$!"
		prefix += "\necho \"local = 10.0.0.0/255.0.0.0 \nserver = 127.0.0.1 \nserver_port = $port\" >> .tmp/" + podUID + "_tsocks.conf"
		prefix += "\nexport TSOCKS_CONF_FILE=.tmp/" + podUID + "_tsocks.conf && export LD_PRELOAD=" + config.Tsockspath
	}

	if podIP, ok := metadata.Annotations["interlink.eu/pod-ip"]; ok {
		prefix += "\n" + "export POD_IP=" + podIP + "\n"
	}

	if config.Commandprefix != "" {
		prefix += "\n" + config.Commandprefix
	}

	if wstunnelClientCommands, ok := metadata.Annotations["interlink.eu/wstunnel-client-commands"]; ok {
		prefix += "\n" + wstunnelClientCommands + "\n"
	}

	if preExecAnnotations, ok := metadata.Annotations["slurm-job.vk.io/pre-exec"]; ok {
		if strings.Contains(preExecAnnotations, "cat <<'EOFMESH' > $TMPDIR/mesh.sh") {
			meshScript, err := extractHeredoc(preExecAnnotations, "EOFMESH")
			if err == nil && meshScript != "" {
				meshPath := filepath.Join(path, "mesh.sh")
				err := os.WriteFile(meshPath, []byte(meshScript), 0755)
				if err != nil {
					prefix += "\n" + preExecAnnotations
				} else {
					preExecWithoutHeredoc := removeHeredoc(preExecAnnotations, "EOFMESH")
					prefix += "\n" + preExecWithoutHeredoc + "\n" + fmt.Sprintf(" %s", meshPath)
				}
				os.Chmod(path+"/mesh.sh", 0774)
			} else {
				prefix += "\n" + preExecAnnotations
			}
		} else {
			prefix += "\n" + preExecAnnotations
		}
	}

	sbatch_common_funcs_macros := `

####
# Functions
####

# Wait for 60 times 2s if the file exist.
waitFileExist() {
  filePath="$1"
  printf "%s\n" "$(date -Is --utc) Checking if file exists: ${filePath} ..."
  i=1
  iMax=60
  while test "${i}" -le "${iMax}" ; do
	if test -e "${filePath}" ; then
	  printf "%s\n" "$(date -Is --utc) attempt ${i}/${iMax} file found ${filePath}"
	  break
	fi
    printf "%s\n" "$(date -Is --utc) attempt ${i}/${iMax} file not found ${filePath}"
	i=$((i + 1))
    sleep 2
  done
}

runInitCtn() {
  ctn="$1"
  shift
  printf "%s\n" "$(date -Is --utc) Running init container ${ctn}..."
  time ( "$@" ) &> ${workingPath}/init-${ctn}.out
  exitCode="$?"
  printf "%s\n" "${exitCode}" > ${workingPath}/init-${ctn}.status
  waitFileExist "${workingPath}/init-${ctn}.status"
  if test "${exitCode}" != 0 ; then
    printf "%s\n" "$(date -Is --utc) InitContainer ${ctn} failed with status ${exitCode}" >&2
    exit "${exitCode}"
  fi
}

runCtn() {
  ctn="$1"
  shift
  time ( "$@" ) >> ${workingPath}/run-${ctn}.out 2>&1 &
  pid="$!"
  printf "%s\n" "$(date -Is --utc) Running in background ${ctn} pid ${pid}..."
  pidCtns="${pidCtns} ${pid}:${ctn}"
}

waitCtns() {
  for pidCtn in ${pidCtns} ; do
    pid="${pidCtn%:*}"
    ctn="${pidCtn#*:}"
    printf "%s\n" "$(date -Is --utc) Waiting for container ${ctn} pid ${pid}..."
    wait "${pid}"
    exitCode="$?"
    printf "%s\n" "${exitCode}" > "${workingPath}/run-${ctn}.status"
    printf "%s\n" "$(date -Is --utc) Container ${ctn} pid ${pid} ended with status ${exitCode}."
	waitFileExist "${workingPath}/run-${ctn}.status"
  done
  for filestatus in $(ls ${workingPath}/*.status 2>/dev/null) ; do
    exitCode=$(cat "$filestatus")
    test "${highestExitCode}" -lt "${exitCode}" && highestExitCode="${exitCode}"
  done
}

endScript() {
  printf "%s\n" "$(date -Is --utc) End of script, highest exit code ${highestExitCode}..."
  exit "${highestExitCode}"
}

####
# Main
####

highestExitCode=0

	`

	var stringToBeWritten strings.Builder

	// Write the shebang + prefix (file-setup) + helper functions all into job.sh.
	stringToBeWritten.WriteString("#!" + config.BashPath + "\n")
	// NOTE: prefix must be separated from the rest by a newline so that a
	// base64 heredoc end-marker is not accidentally appended to the next line.
	if prefix != "" {
		stringToBeWritten.WriteString(prefix + "\n")
	}
	stringToBeWritten.WriteString(sbatch_common_funcs_macros)

	stringToBeWritten.WriteString("\nprintf '%s\n' \"This pod ")
	stringToBeWritten.WriteString(pod.Name)
	stringToBeWritten.WriteString("/")
	stringToBeWritten.WriteString(podUID)
	stringToBeWritten.WriteString(" is running directly via apptainer.\"")

	stringToBeWritten.WriteString("\nexport workingPath=")
	stringToBeWritten.WriteString(path)
	stringToBeWritten.WriteString("\n")
	stringToBeWritten.WriteString("\nexport SANDBOX=")
	stringToBeWritten.WriteString(path)
	stringToBeWritten.WriteString("\n")

	// Generate probe cleanup script if any probes exist.
	var hasProbes bool
	for _, containerCommand := range commands {
		if len(containerCommand.readinessProbes) > 0 || len(containerCommand.livenessProbes) > 0 || len(containerCommand.startupProbes) > 0 {
			hasProbes = true
			break
		}
	}
	if hasProbes && config.EnableProbes {
		for _, containerCommand := range commands {
			if len(containerCommand.readinessProbes) > 0 || len(containerCommand.livenessProbes) > 0 || len(containerCommand.startupProbes) > 0 {
				cleanupScript := generateProbeCleanupScript(containerCommand.containerName, containerCommand.readinessProbes, containerCommand.livenessProbes, containerCommand.startupProbes)
				stringToBeWritten.WriteString(cleanupScript)
				break
			}
		}
	}

	// Inject SIGTERM trap for preStop lifecycle hooks.
	if trapScript := generatePreStopTrap(config, commands); trapScript != "" {
		stringToBeWritten.WriteString(trapScript)
	}

	for _, containerCommand := range commands {
		stringToBeWritten.WriteString("\n")

		if containerCommand.isInitContainer {
			stringToBeWritten.WriteString("runInitCtn ")
		} else {
			if postStartScript := generatePostStartScript(config, containerCommand); postStartScript != "" {
				stringToBeWritten.WriteString(postStartScript)
				containerCommand.runtimeCommand = injectTmpBindMount(containerCommand.runtimeCommand)
			}
			stringToBeWritten.WriteString("runCtn ")
		}
		stringToBeWritten.WriteString(containerCommand.containerName)
		stringToBeWritten.WriteString(" ")
		stringToBeWritten.WriteString(strings.Join(containerCommand.runtimeCommand[:], " "))

		if containerCommand.containerCommand != nil {
			for _, commandEntry := range containerCommand.containerCommand {
				stringToBeWritten.WriteString(" ")
				stringToBeWritten.WriteString(shellescape.Quote(commandEntry))
			}
		}
		if containerCommand.containerArgs != nil {
			for _, argsEntry := range containerCommand.containerArgs {
				stringToBeWritten.WriteString(" ")
				stringToBeWritten.WriteString(shellescape.Quote(argsEntry))
			}
		}

		// Generate probe scripts if enabled.
		if config.EnableProbes && !containerCommand.isInitContainer && (len(containerCommand.readinessProbes) > 0 || len(containerCommand.livenessProbes) > 0 || len(containerCommand.startupProbes) > 0) {
			imageName := containerCommand.containerImage
			if imageName != "" {
				err := storeProbeMetadata(path, containerCommand.containerName, len(containerCommand.readinessProbes), len(containerCommand.livenessProbes), len(containerCommand.startupProbes))
				if err != nil {
					log.G(Ctx).Error("Failed to store probe metadata: ", err)
				}
				probeScript := generateProbeScript(Ctx, config, containerCommand.containerName, imageName, containerCommand.readinessProbes, containerCommand.livenessProbes, containerCommand.startupProbes)
				stringToBeWritten.WriteString("\n")
				stringToBeWritten.WriteString(probeScript)
			}
		}
	}

	stringToBeWritten.WriteString("\n")
	stringToBeWritten.WriteString(postfix)
	stringToBeWritten.WriteString("\nwaitCtns\nendScript\n\n")

	_, err = f.WriteString(stringToBeWritten.String())
	if err != nil {
		log.G(Ctx).Error(err)
		return "", err
	} else {
		log.G(Ctx).Debug("---- Written job.sh file")
	}

	duration := time.Now().UnixMicro() - start
	span.AddEvent("Produced Apptainer script", trace.WithAttributes(
		attribute.String("produceapptainerscript.path", f.Name()),
		attribute.Int64("produceapptainerscript.duration", duration),
	))

	return f.Name(), nil
}

// storeJobInfo writes JobID.jid, PodNamespace.ns and PodUID.uid files and
// registers the JID entry in the shared JIDs map.
func storeJobInfo(Ctx context.Context, pod v1.Pod, JIDs *map[string]*JidStruct, pid string, path string) (string, error) {
	fJID, err := os.Create(path + "/JobID.jid")
	if err != nil {
		log.G(Ctx).Error("Can't create jid_file")
		return "", err
	}
	defer fJID.Close()

	fNS, err := os.Create(path + "/PodNamespace.ns")
	if err != nil {
		log.G(Ctx).Error("Can't create namespace_file")
		return "", err
	}
	defer fNS.Close()

	fUID, err := os.Create(path + "/PodUID.uid")
	if err != nil {
		log.G(Ctx).Error("Can't create PodUID_file")
		return "", err
	}
	defer fUID.Close()

	_, err = fJID.WriteString(pid)
	if err != nil {
		log.G(Ctx).Error(err)
		return "", err
	}

	(*JIDs)[string(pod.UID)] = &JidStruct{PodUID: string(pod.UID), PodNamespace: pod.Namespace, JID: pid}
	log.G(Ctx).Info("Job PID is: " + (*JIDs)[string(pod.UID)].JID)

	_, err = fNS.WriteString(pod.Namespace)
	if err != nil {
		log.G(Ctx).Error(err)
		return "", err
	}

	_, err = fUID.WriteString(string(pod.UID))
	if err != nil {
		log.G(Ctx).Error(err)
		return "", err
	}

	return (*JIDs)[string(pod.UID)].JID, nil
}

// removeJID removes a JID entry from the shared map.
func removeJID(podUID string, JIDs *map[string]*JidStruct) {
	delete(*JIDs, podUID)
}

// deleteContainer kills the job process (if still running) and cleans up files.
func deleteContainer(Ctx context.Context, config ApptainerConfig, podUID string, JIDs *map[string]*JidStruct, path string) error {
	log.G(Ctx).Info("- Deleting Job for pod " + podUID)
	span := trace.SpanFromContext(Ctx)

	if checkIfJidExists(Ctx, JIDs, podUID) {
		jidStr := (*JIDs)[podUID].JID
		pid, err := strconv.Atoi(strings.TrimSpace(jidStr))
		if err != nil {
			log.G(Ctx).Warning("Could not parse PID from JID: ", jidStr, " error: ", err)
		} else {
			// Kill the entire process group so child processes are also terminated.
			killProcessGroup(Ctx, pid)
		}
	}

	jid := ""
	if entry, ok := (*JIDs)[podUID]; ok {
		jid = entry.JID
	}
	removeJID(podUID, JIDs)

	span.SetAttributes(
		attribute.String("delete.pod.uid", podUID),
		attribute.String("delete.jid", jid),
	)

	errFirstAttempt := os.RemoveAll(path)
	if errFirstAttempt != nil {
		log.G(Ctx).Debug("Attempt 1 of deletion failed (logs may still be open), waiting 5s... Error: ", errFirstAttempt)
		time.Sleep(5 * time.Second)

		errSecondAttempt := os.RemoveAll(path)
		if errSecondAttempt != nil {
			log.G(Ctx).Error("Attempt 2 of deletion failed: ", errSecondAttempt)
			span.AddEvent("Failed to delete Job " + jid + " for Pod " + podUID)
			return errSecondAttempt
		} else {
			log.G(Ctx).Info("Attempt 2 of deletion succeeded!")
		}
	}
	span.AddEvent("Job " + jid + " for Pod " + podUID + " successfully deleted")
	return nil
}

// checkIfJidExists returns true when the pod UID has an entry in the JIDs map.
func checkIfJidExists(ctx context.Context, JIDs *map[string]*JidStruct, uid string) bool {
	span := trace.SpanFromContext(ctx)
	_, ok := (*JIDs)[uid]
	if ok {
		return true
	}
	span.AddEvent("Span for PodUID " + uid + " doesn't exist")
	return false
}

// getExitCode reads the container exit code from its .status file.
// When the file is absent (e.g. job killed before shell wrote it) the
// provided fallbackCode is used and the file is created so subsequent
// calls remain consistent.
func getExitCode(ctx context.Context, path string, ctName string, fallbackCode string, sessionContextMessage string) (int32, error) {
	statusFilePath := path + "/run-" + ctName + ".status"
	exitCode, err := os.ReadFile(statusFilePath)
	if err != nil {
		statusFilePath = path + "/init-" + ctName + ".status"
		exitCode, err = os.ReadFile(statusFilePath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				log.G(ctx).Warning(sessionContextMessage, "file ", statusFilePath, " not found. Using fallback exit code: ", fallbackCode)

				exitCodeInt, errAtoi := strconv.Atoi(fallbackCode)
				if errAtoi != nil {
					return 11, fmt.Errorf(sessionContextMessage+"error during Atoi() of fallbackCode %s: %w", fallbackCode, errAtoi)
				}
				errWriteFile := os.WriteFile(statusFilePath, []byte(fallbackCode), 0644)
				if errWriteFile != nil {
					return 12, fmt.Errorf(sessionContextMessage+"error during WriteFile() of status file %s: %w", statusFilePath, errWriteFile)
				}
				return int32(exitCodeInt), nil
			}
			return 21, fmt.Errorf(sessionContextMessage+"error during ReadFile() of %s: %w", statusFilePath, err)
		}
	}
	exitCodeInt, err := strconv.Atoi(strings.TrimSpace(string(exitCode)))
	if err != nil {
		log.G(ctx).Error(err)
		return 0, err
	}
	return int32(exitCodeInt), nil
}

// prepareRuntimeCommand builds the apptainer exec/run command prefix for a container.
func prepareRuntimeCommand(config ApptainerConfig, container v1.Container, metadata metav1.ObjectMeta) []string {
	apptainerMounts := ""
	if mounts, ok := metadata.Annotations["slurm-job.vk.io/singularity-mounts"]; ok {
		apptainerMounts = mounts
	}
	// Support the apptainer-native annotation as well.
	if mounts, ok := metadata.Annotations["apptainer-job.vk.io/mounts"]; ok {
		apptainerMounts = mounts
	}

	apptainerOptions := ""
	if opts, ok := metadata.Annotations["slurm-job.vk.io/singularity-options"]; ok {
		apptainerOptions = opts
	}
	if opts, ok := metadata.Annotations["apptainer-job.vk.io/options"]; ok {
		apptainerOptions = opts
	}

	// apptainer exec overrides the entrypoint; apptainer run honours it.
	apptainerCommand := "run"
	if len(container.Command) != 0 {
		apptainerCommand = "exec"
	}

	commstr1 := []string{config.ApptainerPath, apptainerCommand}
	commstr1 = append(commstr1, config.ApptainerDefaultOptions...)
	commstr1 = append(commstr1, apptainerMounts, apptainerOptions)
	return commstr1
}

// prepareImage resolves the final image URI, adding the configured prefix when
// the image has no URI scheme and is not an absolute path.
func prepareImage(Ctx context.Context, config ApptainerConfig, metadata metav1.ObjectMeta, containerImage string) string {
	image := containerImage
	imagePrefix := config.ImagePrefix

	imagePrefixAnnotationFound := false
	if imagePrefixAnnotation, ok := metadata.Annotations["slurm-job.vk.io/image-root"]; ok {
		imagePrefix = imagePrefixAnnotation
		imagePrefixAnnotationFound = true
	}
	if imagePrefixAnnotation, ok := metadata.Annotations["apptainer-job.vk.io/image-root"]; ok {
		imagePrefix = imagePrefixAnnotation
		imagePrefixAnnotationFound = true
	}
	log.G(Ctx).Info("imagePrefix from annotation? ", imagePrefixAnnotationFound, " value: ", imagePrefix)

	hasScheme := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+\-.]*://`).MatchString(image)
	if strings.HasPrefix(image, "/") {
		log.G(Ctx).Warningf("image set to %s is an absolute path. Prefix won't be added.", image)
	} else if hasScheme {
		log.G(Ctx).Infof("image %s already has a URI scheme. Prefix won't be added.", image)
	} else if imagePrefix != "" {
		image = imagePrefix + containerImage
	} else {
		log.G(Ctx).Warningf("imagePrefix is empty and image %s has no URI scheme. Image will be used as-is.", image)
	}
	return image
}

func mountDataSimpleVolume(
	Ctx context.Context,
	container *v1.Container,
	path string,
	span trace.Span,
	volumeMount v1.VolumeMount,
	mountDataFiles map[string][]byte,
	start int64,
	volumeType string,
	fileMode os.FileMode,
) ([]string, []string, error) {
	span.AddEvent("Preparing " + volumeType + " mount")

	var volumesHostToContainerPaths []string
	var envVarNames []string

	err := os.RemoveAll(path + "/" + volumeType + "/" + volumeMount.Name)
	if err != nil {
		log.G(Ctx).Error("Unable to delete root folder")
		return []string{}, nil, err
	}

	log.G(Ctx).Info("--- Mounting ", volumeType, ": "+volumeMount.Name)
	podVolumeDir := filepath.Join(path, volumeType, volumeMount.Name)

	for key := range mountDataFiles {
		fullPath := filepath.Join(podVolumeDir, key)
		hexString := stringToHex(fullPath)
		mode := ""
		if volumeMount.ReadOnly {
			mode = ":ro"
		} else {
			mode = ":rw"
		}

		var containerPath string
		if volumeMount.SubPath != "" {
			containerPath = volumeMount.MountPath
		} else {
			containerPath = filepath.Join(volumeMount.MountPath, key)
		}

		bind := fullPath + ":" + containerPath + mode + " "
		volumesHostToContainerPaths = append(volumesHostToContainerPaths, bind)

		if os.Getenv("SHARED_FS") != "true" {
			currentEnvVarName := string(container.Name) + "_" + volumeType + "_" + hexString
			log.G(Ctx).Debug("---- Setting env " + currentEnvVarName + " to mount the file later")
			err = os.Setenv(currentEnvVarName, string(mountDataFiles[key]))
			if err != nil {
				log.G(Ctx).Error("--- Shared FS disabled, unable to set ENV for ", volumeType, "key: ", key, " env name: ", currentEnvVarName)
				return []string{}, nil, err
			}
			envVarNames = append(envVarNames, currentEnvVarName)
		}
	}

	if os.Getenv("SHARED_FS") == "true" {
		log.G(Ctx).Info("--- Shared FS enabled, files will be directly created before the job submission")
		err := os.MkdirAll(podVolumeDir, os.FileMode(0755)|os.ModeDir)
		if err != nil {
			return []string{}, nil, fmt.Errorf("could not create whole directory of %s root cause %w", podVolumeDir, err)
		}
		log.G(Ctx).Debug("--- Created folder ", podVolumeDir)
		log.G(Ctx).Debug("--- Writing ", volumeType, " files")
		for k, v := range mountDataFiles {
			fullPath := filepath.Join(podVolumeDir, k)
			err := os.WriteFile(fullPath, v, fileMode)
			if err != nil {
				log.G(Ctx).Errorf("Could not write %s file %s", volumeType, fullPath)
				os.RemoveAll(fullPath)
				return []string{}, nil, err
			} else {
				log.G(Ctx).Debugf("--- Written %s file %s", volumeType, fullPath)
			}
		}
	}

	duration := time.Now().UnixMicro() - start
	span.AddEvent("Prepared "+volumeType+" mounts", trace.WithAttributes(
		attribute.String("mountdata.container.name", container.Name),
		attribute.Int64("mountdata.duration", duration),
		attribute.StringSlice("mountdata.container."+volumeType, volumesHostToContainerPaths)))
	return volumesHostToContainerPaths, envVarNames, nil
}

// mountData creates files on disk (or sets up env vars for non-shared-FS mode)
// for ConfigMaps, Secrets, projected volumes and EmptyDirs.
func mountData(Ctx context.Context, config ApptainerConfig, container *v1.Container, retrievedDataObject interface{}, volumeMount v1.VolumeMount, volume v1.Volume, path string) ([]string, []string, error) {
	span := trace.SpanFromContext(Ctx)
	start := time.Now().UnixMicro()
	if config.ExportPodData {
		switch retrievedDataObjectCasted := retrievedDataObject.(type) {
		case v1.ConfigMap:
			var volumeType string
			var defaultMode *int32
			if volume.ConfigMap != nil {
				volumeType = "configMaps"
				defaultMode = volume.ConfigMap.DefaultMode
			} else if volume.Projected != nil {
				volumeType = "projectedVolumeMaps"
				defaultMode = volume.Projected.DefaultMode
			}

			log.G(Ctx).Debugf("in mountData() volume found: %s type: %s", volumeMount.Name, volumeType)

			mountDataConfigMapsAsBytes := make(map[string][]byte)
			for key := range retrievedDataObjectCasted.Data {
				mountDataConfigMapsAsBytes[key] = normalizeVolumeFileContent(retrievedDataObjectCasted.Data[key])
			}
			fileMode := os.FileMode(*defaultMode)
			return mountDataSimpleVolume(Ctx, container, path, span, volumeMount, mountDataConfigMapsAsBytes, start, volumeType, fileMode)

		case v1.Secret:
			volumeType := "secrets"
			log.G(Ctx).Debugf("in mountData() volume found: %s type: %s", volumeMount.Name, volumeType)

			fileMode := os.FileMode(*volume.Secret.DefaultMode)
			return mountDataSimpleVolume(Ctx, container, path, span, volumeMount, retrievedDataObjectCasted.Data, start, volumeType, fileMode)

		case string:
			span.AddEvent("Preparing EmptyDirs mount")
			var edPaths []string
			if volume.EmptyDir != nil {
				log.G(Ctx).Debugf("in mountData() volume found: %s type: emptyDir", volumeMount.Name)

				var edPath string
				edPath = filepath.Join(path, "emptyDirs", volume.Name)
				log.G(Ctx).Info("-- Creating EmptyDir in ", edPath)
				err := os.MkdirAll(edPath, os.FileMode(0755)|os.ModeDir)
				if err != nil {
					return []string{}, nil, fmt.Errorf("could not create whole directory of %s root cause %w", edPath, err)
				}
				log.G(Ctx).Debug("-- Created EmptyDir in ", edPath)

				mode := ""
				if volumeMount.ReadOnly {
					mode = ":ro"
				} else {
					mode = ":rw"
				}
				edPath += (":" + volumeMount.MountPath + mode + " ")
				edPaths = append(edPaths, " --bind "+edPath+" ")
			}
			duration := time.Now().UnixMicro() - start
			span.AddEvent("Prepared emptydir mounts", trace.WithAttributes(
				attribute.String("mountdata.container.name", container.Name),
				attribute.Int64("mountdata.duration", duration),
				attribute.StringSlice("mountdata.container.emptydirs", edPaths)))
			return edPaths, nil, nil

		default:
			log.G(Ctx).Warningf("in mountData() volume %s with unknown retrievedDataObject", volumeMount.Name)
		}
	}
	return nil, nil, nil
}
