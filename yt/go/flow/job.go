package flow

import (
	"errors"
	"slices"
	"strings"
	"sync"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

var (
	// ErrInvalidStateName reports an external state name that is not an absolute path.
	ErrInvalidStateName = xerrors.NewSentinel("invalid state name")

	// ErrUnknownState reports a state the computation spec does not declare.
	ErrUnknownState = xerrors.NewSentinel("undeclared state")

	// ErrParameterNotFound reports a parameter absent from a computation spec.
	ErrParameterNotFound = xerrors.NewSentinel("parameter not found")
)

// Parameters is the parameters map of a computation spec, left undecoded.
type Parameters map[string]yson.RawValue

// Has reports whether the named parameter is set.
func (p Parameters) Has(name string) bool {
	_, ok := p[name]
	return ok
}

// Names returns the names of the parameters set, sorted.
func (p Parameters) Names() []string {
	return sortedKeys(p)
}

// Get deserializes the named parameter into dst.
func (p Parameters) Get(name string, dst any) error {
	raw, ok := p[name]
	if !ok {
		return xerrors.Errorf("flow: %w: %q", ErrParameterNotFound, name)
	}
	if err := yson.Unmarshal(raw, dst); err != nil {
		return xerrors.Errorf("flow: parameter %q: %w", name, err)
	}
	return nil
}

type computationSpec struct {
	Parameters            Parameters               `yson:"parameters"`
	GroupBySchema         schema.Schema            `yson:"group_by_schema"`
	ExternalStateManagers map[string]yson.RawValue `yson:"external_state_managers"`
	ExternalStateJoiners  map[string]yson.RawValue `yson:"external_state_joiners"`
}

// Job is the configuration of one computation partition.
type Job struct {
	id            guid.GUID
	computationID string
	streams       StreamSpecs
	groupBySchema Schema

	staticParameters  Parameters
	dynamicParameters Parameters

	internalStates       []string
	externalStates       []string
	joinedExternalStates []string
}

// NewJob parses the static and dynamic computation specs.
func NewJob(id guid.GUID, computationID string, streams StreamSpecs, staticSpec, dynamicSpec []byte) (*Job, error) {
	var static computationSpec
	if err := yson.Unmarshal(staticSpec, &static); err != nil {
		return nil, xerrors.Errorf("flow: job %v: parsing static spec: %w", id, err)
	}

	var dynamic computationSpec
	if err := yson.Unmarshal(dynamicSpec, &dynamic); err != nil {
		return nil, xerrors.Errorf("flow: job %v: parsing dynamic spec: %w", id, err)
	}

	internalStates, err := internalStateNames(static.Parameters)
	if err != nil {
		return nil, xerrors.Errorf("flow: job %v: %w", id, err)
	}
	groupBySchema := NewSchema(static.GroupBySchema)
	if err := groupBySchema.validate(); err != nil {
		return nil, xerrors.Errorf("flow: job %v: group-by schema: %w", id, err)
	}

	return &Job{
		id:                   id,
		computationID:        computationID,
		streams:              streams,
		groupBySchema:        groupBySchema,
		staticParameters:     static.Parameters,
		dynamicParameters:    dynamic.Parameters,
		internalStates:       internalStates,
		externalStates:       sortedKeys(static.ExternalStateManagers),
		joinedExternalStates: sortedKeys(static.ExternalStateJoiners),
	}, nil
}

// ID returns the job id.
func (j *Job) ID() guid.GUID {
	return j.id
}

// ComputationID returns the id of the computation this job is a partition of.
func (j *Job) ComputationID() string {
	return j.computationID
}

// StreamSpecs returns the streams the job exchanges messages on.
func (j *Job) StreamSpecs() StreamSpecs {
	return j.streams
}

// GroupBySchema returns the key schema.
func (j *Job) GroupBySchema() Schema {
	return j.groupBySchema
}

// StaticParameters returns immutable static parameters.
func (j *Job) StaticParameters() Parameters {
	return j.staticParameters
}

// DynamicParameters returns immutable dynamic parameters.
func (j *Job) DynamicParameters() Parameters {
	return j.dynamicParameters
}

// InternalStateNames returns sorted internal state names; callers must not modify the slice.
func (j *Job) InternalStateNames() []string {
	return j.internalStates
}

// ExternalStateNames returns sorted owned external state names; callers must not modify the slice.
func (j *Job) ExternalStateNames() []string {
	return j.externalStates
}

// JoinedExternalStateNames returns sorted joined state names; callers must not modify the slice.
func (j *Job) JoinedExternalStateNames() []string {
	return j.joinedExternalStates
}

// ValidateInternalStateName checks that an internal state is declared.
func (j *Job) ValidateInternalStateName(name string) error {
	if !slices.Contains(j.internalStates, name) {
		return xerrors.Errorf("flow: %w: internal state %q, declared: %v",
			ErrUnknownState, name, j.internalStates)
	}
	return nil
}

// ValidateExternalStateName checks that an owned external state is declared.
func (j *Job) ValidateExternalStateName(name string) error {
	if err := validateExternalStateName(name); err != nil {
		return err
	}
	if !slices.Contains(j.externalStates, name) {
		return xerrors.Errorf("flow: %w: external state %q, declared: %v",
			ErrUnknownState, name, j.externalStates)
	}
	return nil
}

// ValidateJoinedExternalStateName checks that a joined external state is declared.
func (j *Job) ValidateJoinedExternalStateName(name string) error {
	if err := validateExternalStateName(name); err != nil {
		return err
	}
	if !slices.Contains(j.joinedExternalStates, name) {
		return xerrors.Errorf("flow: %w: joined external state %q, declared: %v",
			ErrUnknownState, name, j.joinedExternalStates)
	}
	return nil
}

func validateExternalStateName(name string) error {
	switch {
	case name == "":
		return xerrors.Errorf("flow: %w: name is empty", ErrInvalidStateName)
	case !strings.HasPrefix(name, "/"):
		return xerrors.Errorf("flow: %w: %q does not start with '/'", ErrInvalidStateName, name)
	case name == "/":
		return xerrors.Errorf("flow: %w: %q is the root", ErrInvalidStateName, name)
	case strings.HasSuffix(name, "/"):
		return xerrors.Errorf("flow: %w: %q ends with '/'", ErrInvalidStateName, name)
	case strings.Contains(name, "//"):
		return xerrors.Errorf("flow: %w: %q contains two adjacent '/'", ErrInvalidStateName, name)
	}
	return nil
}

func internalStateNames(parameters Parameters) ([]string, error) {
	var names []string
	if err := parameters.Get("internal_states", &names); err != nil {
		if errors.Is(err, ErrParameterNotFound) {
			return nil, nil
		}
		return nil, err
	}
	slices.Sort(names)
	return slices.Compact(names), nil
}

func sortedKeys(m map[string]yson.RawValue) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// jobCache is the registry of jobs owned by the worker: entries are created
// and updated by PutJob and removed by RemoveJob, so an entry lives exactly as
// long as its job.
type jobCache struct {
	mu   sync.Mutex
	jobs map[guid.GUID]*cachedJob
}

type cachedJob struct {
	job                 *Job
	pendingCPUTime      int64
	lastRequestID       guid.GUID
	lastResponseCPUTime int64
	hasLastRequest      bool

	lastMemoryRequestID     guid.GUID
	lastResponseMemoryUsage int64
	hasLastMemoryRequest    bool
}

func newJobCache() *jobCache {
	return &jobCache{
		jobs: map[guid.GUID]*cachedJob{},
	}
}

func (c *jobCache) Get(id guid.GUID) (*Job, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.jobs[id]
	if !ok {
		return nil, false
	}
	return entry.job, true
}

// Put registers or replaces a job.
func (c *jobCache) Put(job *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.jobs[job.ID()]; ok {
		entry.job = job
		return
	}
	c.jobs[job.ID()] = &cachedJob{job: job}
}

func (c *jobCache) AddCPUTime(id guid.GUID, cpuTime int64) {
	if cpuTime <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.jobs[id]
	if !ok {
		return
	}
	entry.pendingCPUTime += cpuTime
}

func (c *jobCache) ResponseCPUTime(id, requestID guid.GUID) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.jobs[id]
	if !ok {
		return 0
	}
	if entry.hasLastRequest && entry.lastRequestID == requestID {
		return entry.lastResponseCPUTime
	}

	cpuTime := entry.pendingCPUTime
	entry.pendingCPUTime = 0
	entry.lastRequestID = requestID
	entry.lastResponseCPUTime = cpuTime
	entry.hasLastRequest = true
	return cpuTime
}

func (c *jobCache) ResponseMemoryUsage(id, requestID guid.GUID, memoryUsage int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.jobs[id]
	if !ok {
		return 0
	}
	if entry.hasLastMemoryRequest && entry.lastMemoryRequestID == requestID {
		return entry.lastResponseMemoryUsage
	}

	entry.lastMemoryRequestID = requestID
	entry.lastResponseMemoryUsage = memoryUsage
	entry.hasLastMemoryRequest = true
	return memoryUsage
}

// Delete removes a job; unknown ids are ignored (removal is idempotent).
func (c *jobCache) Delete(id guid.GUID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.jobs, id)
}

func (c *jobCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.jobs)
}

func (c *jobCache) ActiveIDs() []guid.GUID {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]guid.GUID, 0, len(c.jobs))
	for id := range c.jobs {
		ids = append(ids, id)
	}
	return ids
}
