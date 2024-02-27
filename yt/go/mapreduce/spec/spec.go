// package spec defines specification of YT operation.
//
// See https://wiki.yandex-team.ru/yt/userdoc/operations/
package spec

import (
	"github.com/mitchellh/copystructure"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type File struct {
	FileName            string `yson:"file_name,attr,omitempty"`
	Format              any    `yson:"format,attr,omitempty"`
	Executable          bool   `yson:"executable,attr,omitempty"`
	BypassArtifactCache bool   `yson:"bypass_artifact_cache,attr,omitempty"`

	CypressPath ypath.Path `yson:",value"`
}

type DiskRequest struct {
	DiskSpace  int64  `yson:"disk_space,omitempty"`
	InodeCount int64  `yson:"inode_count,omitempty"`
	Account    string `yson:"account,omitempty"`
	MediumName string `yson:"medium_name,omitempty"`
}

type UserScript struct {
	// Command specifies shell command that would be executed inside job.
	//
	// mapreduce package uses command for it's own purpose. User should not set this field.
	Command string `yson:"command"`

	Format             any               `yson:"format,omitempty"`
	InputFormat        any               `yson:"input_format,omitempty"`
	OutputFormat       any               `yson:"output_format,omitempty"`
	Environment        map[string]string `yson:"environment,omitempty"`
	FilePaths          []File            `yson:"file_paths,omitempty"`
	LayerPaths         []ypath.Path      `yson:"layer_paths,omitempty"`
	MakeRootFSWritable bool              `yson:"make_rootfs_writable,omitempty"`

	TmpfsPath string `yson:"tmpfs_path,omitempty"`
	TmpfsSize int64  `yson:"tmpfs_size,omitempty"`
	CopyFiles bool   `yson:"copy_files,omitempty"`

	// CPULimit corresponds to cpu_limit job setting.
	//
	// This setting results in GOMAXPROCS set to max(1, ceil(CPULimit)).
	CPULimit            float32       `yson:"cpu_limit,omitempty"`
	MemoryLimit         int64         `yson:"memory_limit,omitempty"`
	MemoryReserveFactor float64       `yson:"memory_reserve_factor,omitempty"`
	GPULimit            int           `yson:"gpu_limit,omitempty"`
	JobTimeLimit        yson.Duration `yson:"job_time_limit,omitempty"`
	DiskRequest         *DiskRequest  `yson:"disk_request,omitempty"`

	NetworkProject string `yson:"network_project,omitempty"`

	EnablePorto            string         `yson:"enable_porto,omitempty"`
	UsePortoMemoryTracking *bool          `yson:"use_porto_memory_tracking,omitempty"`
	Monitoring             map[string]any `yson:"monitoring,omitempty"`

	// Following fields are used only in vanilla operations.
	JobCount         int           `yson:"job_count,omitempty"`
	OutputTablePaths []ypath.YPath `yson:"output_table_paths,omitempty"`
	JobIO            *JobIO        `yson:"job_io,omitempty"`
}

type ResourceLimits struct {
	UserSlots int   `yson:"user_slots,omitempty"`
	CPU       int   `yson:"cpu,omitempty"`
	Memory    int64 `yson:"memory,omitempty"`
}

type ControlAttributes struct {
	EnableTableIndex bool `yson:"enable_table_index"`
	EnableRowIndex   bool `yson:"enable_row_index"`
	EnableRangeIndex bool `yson:"enable_range_index"`
	EnableKeySwitch  bool `yson:"enable_key_switch"`
}

type JobIO struct {
	TableReader       any                `yson:"table_reader,omitempty"`
	TableWriter       any                `yson:"table_writer,omitempty"`
	ControlAttributes *ControlAttributes `yson:"control_attributes,omitempty"`
}

type JobCPUMonitor struct {
	StartDelay            yson.Duration `yson:"start_delay,omitempty"`
	CheckPeriod           yson.Duration `yson:"check_period,omitempty"`
	IncreaseCoefficient   float64       `yson:"increase_coefficient,omitempty"`
	DecreaseCoefficient   float64       `yson:"decrease_coefficient,omitempty"`
	SmoothingFactor       float64       `yson:"smoothing_factor,omitempty"`
	VoteWindowSize        int           `yson:"vote_window_size,omitempty"`
	VoteDecisionThreshold int           `yson:"vote_decision_threshold,omitempty"`
	MinCPULimit           float64       `yson:"min_cpu_limit,omitempty"`
	EnableCPUReclaim      bool          `yson:"enable_cpu_reclaim"`
}

const (
	AutoMergeRelaxed  = "relaxed"
	AutoMergeEconomic = "economic"
	AutoMergeDisabled = "disabled"
	AutoMergeManual   = "manual"
)

type AutoMerge struct {
	Mode string `yson:"mode"`

	// Setting for manual mode.
	MaxIntermediateChunkCount int `yson:"max_intermediate_chunk_count,omitempty"`
	ChunkCountPerMergeJob     int `yson:"chunk_count_per_merge_job,omitempty"`
	ChunkSizeThreshold        int `yson:"chunk_size_threshold,omitempty"`
}

type Spec struct {
	Type yt.OperationType `yson:"-"`

	Title       string         `yson:"title,omitempty"`
	StartedBy   map[string]any `yson:"started_by,omitempty"`
	Annotations map[string]any `yson:"annotations"`
	Description map[string]any `yson:"description,omitempty"`

	Pool                string   `yson:"pool,omitempty"`
	Weight              float64  `yson:"weight,omitempty"`
	PoolTrees           []string `yson:"pool_trees,omitempty"`
	SchedulingTagFilter string   `yson:"scheduling_tag_filter,omitempty"`
	TentativePoolTrees  []string `yson:"tentative_pool_trees,omitempty"`

	ResourceLimits *ResourceLimits `yson:"resource_limits,omitempty"`

	SecureVault map[string]string `yson:"secure_vault,omitempty"`

	InputTablePaths        []ypath.YPath `yson:"input_table_paths,omitempty"`
	OutputTablePaths       []ypath.YPath `yson:"output_table_paths,omitempty"`
	OutputTablePath        ypath.YPath   `yson:"output_table_path,omitempty"`
	MapperOutputTableCount int           `yson:"mapper_output_table_count,omitempty"`

	Atomicity string   `yson:"atomicity,omitempty"`
	Ordered   bool     `yson:"ordered,omitempty"`
	ReduceBy  []string `yson:"reduce_by,omitempty"`
	SortBy    []string `yson:"sort_by,omitempty"`
	JoinBy    []string `yson:"join_by,omitempty"`
	PivotKeys [][]any  `yson:"pivot_keys,omitempty"`

	MergeMode      string   `yson:"mode,omitempty"`
	MergeBy        []string `yson:"merge_by,omitempty"`
	CombineChunks  bool     `yson:"combine_chunks,omitempty"`
	ForceTransform bool     `yson:"force_transform,omitempty"`

	JobCount              int   `yson:"job_count,omitempty"`
	DataSizePerJob        int64 `yson:"data_size_per_job,omitempty"`
	UseColumnarStatistics *bool `yson:"use_columnar_statistics,omitempty"`

	TimeLimit                     yson.Duration `yson:"time_limit,omitempty"`
	MaxFailedJobCount             int           `yson:"max_failed_job_count,omitempty"`
	MaxSpeculativeJobCountPerTask *int          `yson:"max_speculative_job_count_per_task,omitempty"`
	FailOnJobRestart              *bool         `yson:"fail_on_job_restart,omitempty"`
	TryAvoidDuplicatingJobs       *bool         `yson:"try_avoid_duplicating_jobs,omitempty"`
	StderrTablePath               ypath.Path    `yson:"stderr_table_path,omitempty"`
	CoreTablePath                 ypath.Path    `yson:"core_table_path,omitempty"`

	JobCPUMonitor *JobCPUMonitor `yson:"job_cpu_monitor,omitempty"`

	Mapper         *UserScript            `yson:"mapper,omitempty"`
	Reducer        *UserScript            `yson:"reducer,omitempty"`
	ReduceCombiner *UserScript            `yson:"reduce_combiner,omitempty"`
	Tasks          map[string]*UserScript `yson:"tasks,omitempty"`

	JobIO          *JobIO `yson:"job_io,omitempty"`
	MapJobIO       *JobIO `yson:"map_job_io,omitempty"`
	ReduceJobIO    *JobIO `yson:"reduce_job_io,omitempty"`
	PartitionJobIO *JobIO `yson:"partition_job_io,omitempty"`
	MergeJobIO     *JobIO `yson:"merge_job_io,omitempty"`
	SortJobIO      *JobIO `yson:"sort_job_io,omitempty"`

	AutoMerge *AutoMerge `yson:"auto_merge,omitempty"`

	ACL                     []yt.ACE `yson:"acl,omitempty"`
	EnableKeyGuarantee      *bool    `yson:"enable_key_guarantee,omitempty"`
	ConsiderOnlyPrimarySize *bool    `yson:"consider_only_primary_size,omitempty"`

	ClusterName    string `yson:"cluster_name,omitempty"`
	NetworkName    string `yson:"network_name,omitempty"`
	CopyAttributes *bool  `yson:"copy_attributes,omitempty"`

	IntermediateDataReplicationFactor int `yson:"intermediate_data_replication_factor,omitempty"`

	ProbingRatio    int    `yson:"probing_ratio,omitempty"`
	ProbingPoolTree string `yson:"probing_pool_tree,omitempty"`

	InputQuery string `yson:"input_query,omitempty"`
	Alias      string `yson:"alias,omitempty"`
}

func (s *Spec) ReduceByColumns(columns ...string) *Spec {
	s.ReduceBy = columns
	return s
}

func (s *Spec) SortByColumns(columns ...string) *Spec {
	s.SortBy = columns
	return s
}

func (s *Spec) JoinByColumns(columns ...string) *Spec {
	s.JoinBy = columns
	return s
}

func (s *Spec) AddInput(path ypath.YPath) *Spec {
	s.InputTablePaths = append(s.InputTablePaths, path)
	return s
}

func (s *Spec) AddOutput(path ypath.YPath) *Spec {
	s.OutputTablePaths = append(s.OutputTablePaths, path)
	return s
}

func (s *Spec) SetOutput(path ypath.YPath) *Spec {
	s.OutputTablePath = path
	return s
}

func (s *Spec) AddSecureVaultVar(name, value string) *Spec {
	if s.SecureVault == nil {
		s.SecureVault = map[string]string{}
	}

	s.SecureVault[name] = value
	return s
}

func (s *Spec) AddAnnotations(annotations map[string]any) *Spec {
	if s.Annotations == nil {
		s.Annotations = annotations
		return s
	}

	for k, v := range annotations {
		s.Annotations[k] = v
	}

	return s
}

func (s *Spec) Clone() *Spec {
	return copystructure.Must(copystructure.Copy(s)).(*Spec)
}

func (s *Spec) VisitUserScripts(cb func(*UserScript)) {
	if s.Mapper != nil {
		cb(s.Mapper)
	}

	if s.Reducer != nil {
		cb(s.Reducer)
	}

	if s.ReduceCombiner != nil {
		cb(s.ReduceCombiner)
	}

	for _, t := range s.Tasks {
		cb(t)
	}
}

func (s *Spec) PatchUserBinary(path ypath.Path) {
	s.VisitUserScripts(func(u *UserScript) {
		u.FilePaths = append(u.FilePaths, File{
			FileName:    "go-binary",
			CypressPath: path,
			Executable:  true,
		})
	})
}
