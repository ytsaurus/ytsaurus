package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc/cypresspath"
)

// ExportAccessLogRequest is the body of POST /export-access-log.
type ExportAccessLogRequest struct {
	Cluster              string           `json:"cluster"`
	Begin                float64          `json:"begin"`
	End                  *float64         `json:"end,omitempty"`
	OutputPath           string           `json:"output_path"`
	Path                 string           `json:"path"`
	Strict               *bool            `json:"strict,omitempty"`
	MaxAccessTransitions *int64           `json:"max_access_transitions,omitempty"`
	Pool                 *string          `json:"pool,omitempty"`
	UserRegex            string           `json:"user_regex,omitempty"`
	PathRegex            string           `json:"path_regex,omitempty"`
	MethodGroup          ActionSelector   `json:"method_group,omitempty"`
	Scope                ScopeSelector    `json:"scope,omitempty"`
	Metadata             *bool            `json:"metadata,omitempty"`
	UserType             UserTypeSelector `json:"user_type,omitempty"`
}

// ExportParameters are the parameters of an export after normalization and defaults.
type ExportParameters struct {
	Cluster string `json:"cluster" yson:"cluster"`
	Begin   int64  `json:"begin" yson:"begin"`
	// End is the end in the request; it is zero if the request has none.
	End         int64            `json:"end,omitempty" yson:"end,omitempty"`
	OutputPath  string           `json:"output_path" yson:"output_path"`
	Path        string           `json:"path" yson:"path"`
	Strict      bool             `json:"strict,omitempty" yson:"strict,omitempty"`
	Pool        string           `json:"pool" yson:"pool"`
	UserRegex   string           `json:"user_regex,omitempty" yson:"user_regex,omitempty"`
	PathRegex   string           `json:"path_regex,omitempty" yson:"path_regex,omitempty"`
	MethodGroup ActionSelector   `json:"method_group,omitempty" yson:"method_group,omitempty"`
	Scope       ScopeSelector    `json:"scope,omitempty" yson:"scope,omitempty"`
	Metadata    *bool            `json:"metadata,omitempty" yson:"metadata,omitempty"`
	UserType    UserTypeSelector `json:"user_type,omitempty" yson:"user_type,omitempty"`
}

// EffectiveParameters are the requested parameters and the end by which the export runs.
type EffectiveParameters struct {
	ExportParameters
	// EffectiveEnd is the requested end, or the end of the input tables if the request has none.
	EffectiveEnd int64 `json:"effective_end" yson:"effective_end"`
}

type ExportAccessLogResponse struct {
	ExportID      string `json:"export_id"`
	OutputCluster string `json:"output_cluster"`
	OutputPath    string `json:"output_path"`
	Path          string `json:"path"`
	Begin         int64  `json:"begin"`
	End           int64  `json:"end"`
}

type ExportJobCounters struct {
	Total     int64 `json:"total"`
	Running   int64 `json:"running"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
}

type ExportInputStatistics struct {
	RowCount   int64 `json:"row_count" yson:"row_count"`
	DataWeight int64 `json:"data_weight" yson:"data_weight"`
}

// ExportAccessLogStatusResponse contains the response of the started export and the state of its operation.
type ExportAccessLogStatusResponse struct {
	ExportAccessLogResponse
	State yt.OperationState     `json:"state"`
	Jobs  ExportJobCounters     `json:"jobs"`
	Input ExportInputStatistics `json:"input"`
	Error *yterrors.Error       `json:"error,omitempty"`
}

// decodeExportAccessLogRequest decodes one JSON object without unknown fields and rejects any data after it.
func decodeExportAccessLogRequest(body io.Reader) (*ExportAccessLogRequest, error) {
	var request ExportAccessLogRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected data after the JSON body")
		}
		return nil, err
	}
	return &request, nil
}

func normalizeTimestamp(value float64) (int64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("value must be a finite Unix timestamp")
	}
	if value < math.MinInt64 || value >= 1<<63 {
		return 0, fmt.Errorf("value must be within the int64 Unix timestamp range")
	}
	return int64(value), nil
}

// isSchedulerName accepts a single pool or pool tree name without YPath operators.
// The cluster's configurable pool name regex is enforced by YT.
func isSchedulerName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `/\@&*[]{}<>`)
}

// Parameters returns the parameters of the export: timestamps truncated to whole Unix seconds and the default
// pool applied when the request has none. Invalid fields and a missing pool are rejected.
func (r *ExportAccessLogRequest) Parameters(defaultPool string) (ExportParameters, error) {
	parameters := ExportParameters{
		Cluster:     r.Cluster,
		OutputPath:  r.OutputPath,
		Path:        r.Path,
		Strict:      r.Strict != nil && *r.Strict,
		Pool:        defaultPool,
		UserRegex:   r.UserRegex,
		PathRegex:   r.PathRegex,
		MethodGroup: r.MethodGroup,
		Scope:       r.Scope,
		Metadata:    r.Metadata,
		UserType:    r.UserType,
	}
	if r.Cluster == "" {
		return ExportParameters{}, yterrors.Err("cluster is required")
	}
	begin, err := normalizeTimestamp(r.Begin)
	if err != nil {
		return ExportParameters{}, yterrors.Err("invalid begin", err)
	}
	if begin <= 0 {
		return ExportParameters{}, yterrors.Err("begin must be positive after truncation to whole Unix seconds")
	}
	parameters.Begin = begin
	if r.End != nil {
		end, err := normalizeTimestamp(*r.End)
		if err != nil {
			return ExportParameters{}, yterrors.Err("invalid end", err)
		}
		if end <= begin {
			return ExportParameters{}, yterrors.Err("end must be greater than begin after truncation to whole Unix seconds")
		}
		parameters.End = end
	}
	if !cypresspath.IsCanonical(r.OutputPath) {
		return ExportParameters{}, yterrors.Err("the output_path must be an absolute Cypress path of a table in the canonical YPath escaping")
	}
	if r.OutputPath == "/" {
		return ExportParameters{}, yterrors.Err("the output_path must not be the Cypress root")
	}
	if !cypresspath.IsCanonical(r.Path) {
		return ExportParameters{}, yterrors.Err("path must be / or an absolute Cypress path in the canonical YPath escaping")
	}
	if r.Pool != nil && *r.Pool == "" {
		return ExportParameters{}, yterrors.Err("pool must not be empty")
	}
	if r.Pool != nil {
		parameters.Pool = *r.Pool
	}
	if parameters.Pool == "" {
		return ExportParameters{}, yterrors.Err("pool is required when no default export pool is configured")
	}
	for name, expression := range map[string]string{"user_regex": r.UserRegex, "path_regex": r.PathRegex} {
		if _, err := regexp.Compile(expression); err != nil {
			return ExportParameters{}, yterrors.Err(fmt.Sprintf("invalid %s", name), err)
		}
	}
	return parameters, nil
}

func formatExportID(operationCluster string, operationID yt.OperationID) string {
	return operationCluster + ":" + operationID.String()
}

func parseExportID(exportID string) (string, yt.OperationID, error) {
	operationCluster, operationIDString, ok := strings.Cut(exportID, ":")
	if !ok || operationCluster == "" {
		return "", yt.NullOperationID, yterrors.Err("invalid export_id")
	}
	var operationID yt.OperationID
	if err := operationID.UnmarshalText([]byte(operationIDString)); err != nil {
		return "", yt.NullOperationID, yterrors.Err("invalid export_id", err)
	}
	return operationCluster, operationID, nil
}
