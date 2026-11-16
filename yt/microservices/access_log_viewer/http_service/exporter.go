package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

const (
	exportAnnotationKey        = "access_log_export"
	exportOperationAliasPrefix = "*access_log_viewer_export:"
	fullReadPrincipal          = ypath.Path("//sys/access_control_object_namespaces/access_log_viewer/full_read/principal")
	fullReadCheckType          = "access_control_object"
	sourceClientTimeout        = 5 * time.Second
	exportRequestTimeout       = 2 * time.Minute
	maxExportRequestBodySize   = 1 << 20
)

type AccessLogExporterConfig struct {
	OperationCluster   string
	SnapshotRoot       ypath.Path
	LogsTimezone       string
	TokenEnvVariable   string
	DefaultPool        string
	PoolTree           string
	MaxInputTableCount int
}

type AccessLogExporter struct {
	config                 AccessLogExporterConfig
	logger                 *zap.Logger
	logsLocation           *time.Location
	operationClusterClient yt.Client
	authorizer             *Authorizer

	mutex sync.Mutex
	robot string
}

// exportAnnotation is stored in the operation spec and identifies the owner and the parameters of an export.
type exportAnnotation struct {
	Owner               string              `yson:"owner"`
	EffectiveParameters EffectiveParameters `yson:"effective_parameters"`
}

var exportOperationAttributes = []string{"id", "state", "brief_progress", "progress", "result", "authenticated_user", "runtime_parameters"}

var existingExportAttributes = []string{"id", "authenticated_user", "runtime_parameters"}

func newYTClient(proxy, tokenEnvVariable string, logger *zap.Logger, timeout time.Duration) (yt.Client, error) {
	return ythttp.NewClient(&yt.Config{
		Proxy:               proxy,
		Logger:              logger,
		LightRequestTimeout: &timeout,
		Token:               ytmsvc.TokenFromEnvVariable(tokenEnvVariable),
	})
}

// operationClusterNamePattern matches a cluster name, which export_id contains without encoding.
var operationClusterNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// unknownOperationAliasPattern matches the error of get_operation for an alias that no operation has. The error has no
// code of its own.
var unknownOperationAliasPattern = regexp.MustCompile(`[Oo]peration alias is unknown`)

// NewAccessLogExporter validates the configuration without contacting the operation cluster.
func NewAccessLogExporter(config AccessLogExporterConfig, operationClusterClient yt.Client, authorizer *Authorizer, logger *zap.Logger) (*AccessLogExporter, error) {
	if !operationClusterNamePattern.MatchString(config.OperationCluster) {
		return nil, fmt.Errorf("access log export operation cluster %q must be a cluster name", config.OperationCluster)
	}
	if config.DefaultPool != "" && !isSchedulerName(config.DefaultPool) {
		return nil, fmt.Errorf("access log export default pool %q must be a scheduler name", config.DefaultPool)
	}
	if config.PoolTree != "" && !isSchedulerName(config.PoolTree) {
		return nil, fmt.Errorf("access log export pool tree %q must be a scheduler name", config.PoolTree)
	}
	if config.MaxInputTableCount <= 0 {
		return nil, fmt.Errorf("maximum input table count of an access log export must be positive")
	}
	logsLocation, err := time.LoadLocation(config.LogsTimezone)
	if err != nil {
		return nil, fmt.Errorf("invalid access log timezone %q: %w", config.LogsTimezone, err)
	}
	return &AccessLogExporter{
		config:                 config,
		logsLocation:           logsLocation,
		operationClusterClient: operationClusterClient,
		authorizer:             authorizer,
		logger:                 logger,
	}, nil
}

// getRobot returns the cached login of the service robot. WhoAmI runs without the mutex, so concurrent
// first requests may each call it once.
func (e *AccessLogExporter) getRobot(ctx context.Context) (string, error) {
	e.mutex.Lock()
	robot := e.robot
	e.mutex.Unlock()
	if robot != "" {
		return robot, nil
	}
	identity, err := e.operationClusterClient.WhoAmI(ctx, nil)
	if err != nil {
		return "", err
	}
	if identity == nil || identity.Login == "" {
		return "", fmt.Errorf("service robot login is empty")
	}
	e.mutex.Lock()
	e.robot = identity.Login
	e.mutex.Unlock()
	return identity.Login, nil
}

func (e *AccessLogExporter) authorizeExport(ctx context.Context, user, callerService string, parameters *ExportParameters) error {
	allowed, err := e.checkFullRead(ctx, user, parameters.Cluster)
	e.logger.Info("Access log export authorization checked",
		log.String("check_type", fullReadCheckType),
		log.Bool("allowed", allowed),
		log.Error(err),
		log.String("user", user),
		log.String("caller_service", callerService),
		log.String("cluster", parameters.Cluster),
		log.String("path", parameters.Path))
	if err != nil && !yterrors.ContainsResolveError(err) {
		return yterrors.Err("access log export authorization is unavailable", yterrors.Attr("cluster", parameters.Cluster), err)
	}
	if !allowed {
		denial := []any{
			"access log export requires the use permission on the full_read access control object",
			yterrors.Attr("user", user),
			yterrors.Attr("permissions", []yt.Permission{yt.PermissionUse}),
			yterrors.Attr("path", fullReadPrincipal.String()),
			yterrors.Attr("cluster", parameters.Cluster),
		}
		// A missing full_read ACO means that nobody has been granted the export on this cluster.
		if err != nil {
			denial = append(denial, err)
		}
		return yterrors.Err(denial...)
	}
	return nil
}

func (e *AccessLogExporter) checkFullRead(ctx context.Context, user, cluster string) (bool, error) {
	client, err := newYTClient(cluster, e.config.TokenEnvVariable, e.logger, sourceClientTimeout)
	if err != nil {
		return false, fmt.Errorf("cannot create a client for source cluster %q: %w", cluster, err)
	}
	defer client.Stop()
	response, err := client.CheckPermission(ctx, user, yt.PermissionUse, fullReadPrincipal, nil)
	if err != nil {
		return false, err
	}
	return response.Action == yt.ActionAllow, nil
}

func (e *AccessLogExporter) checkPool(ctx context.Context, user, robot, pool, poolTree string) error {
	orchidPath := ypath.Path("//sys/scheduler/orchid/scheduler/pool_trees").
		Child(ypath.EscapeLiteral(poolTree)).Child("pools").Child(ypath.EscapeLiteral(pool))
	var poolInfo struct {
		FullPath    string `yson:"full_path"`
		IsEphemeral bool   `yson:"is_ephemeral"`
	}
	if err := e.operationClusterClient.GetNode(ctx, orchidPath, &poolInfo, nil); err != nil {
		return yterrors.Err("could not read the access log export pool from the scheduler orchid", err)
	}
	if poolInfo.IsEphemeral {
		return yterrors.Err("ephemeral pools are not supported for access log export",
			yterrors.Attr("pool", pool),
			yterrors.Attr("pool_tree", poolTree))
	}
	// The orchid reports no full_path only for the root of the pool tree.
	if poolInfo.FullPath == "" {
		return yterrors.Err("operations cannot start in the root of the pool tree",
			yterrors.Attr("pool", pool),
			yterrors.Attr("pool_tree", poolTree))
	}
	// full_path is escaped by the scheduler, for example "/parent/child".
	poolPath := ypath.Path("//sys/pool_trees").Child(ypath.EscapeLiteral(poolTree)) + ypath.Path(poolInfo.FullPath)
	// The default pool needs no user permission, whether the request names it or omits it.
	if pool != e.config.DefaultPool {
		response, err := e.operationClusterClient.CheckPermission(ctx, user, yt.PermissionUse, poolPath, nil)
		if err != nil {
			return yterrors.Err("could not check access to the requested pool", err)
		}
		if response.Action != yt.ActionAllow {
			return yterrors.Err("use access to the requested pool is denied",
				yterrors.Attr("user", user),
				yterrors.Attr("permissions", []yt.Permission{yt.PermissionUse}),
				yterrors.Attr("path", poolPath.String()))
		}
	}
	response, err := e.operationClusterClient.CheckPermission(ctx, robot, yt.PermissionUse, poolPath, nil)
	if err != nil {
		return yterrors.Err("could not check service robot permissions for the export", err)
	}
	if response.Action != yt.ActionAllow {
		return yterrors.Err("service robot lacks permissions required for the export",
			yterrors.Attr("robot", robot),
			yterrors.Attr("permissions", []yt.Permission{yt.PermissionUse}),
			yterrors.Attr("path", poolPath.String()))
	}
	return nil
}

func (e *AccessLogExporter) authenticateUser(r *http.Request, cluster string) (login, callerService string, err error) {
	user, _, callerService, err := e.authorizer.getUserAndService(r, cluster)
	if err != nil {
		if yterrors.ContainsErrorCode(err, yterrors.CodeInvalidCredentials) {
			return "", "", err
		}
		return "", "", yterrors.Err("access log export authentication is unavailable", err)
	}
	if user == nil || user.Login == "" {
		return "", "", yterrors.Err("access log export authentication returned an empty login")
	}
	return user.Login, callerService, nil
}

// startExport starts the export, or returns the export that already writes to the existing output table.
func (e *AccessLogExporter) startExport(
	ctx context.Context,
	user string,
	callerService string,
	parameters ExportParameters,
) (*ExportAccessLogResponse, error) {
	robot, err := e.getRobot(ctx)
	if err != nil {
		return nil, yterrors.Err("could not get the service robot login", err)
	}
	// The transaction is committed only after all checks, so that a failed check leaves no new table.
	tx, err := e.operationClusterClient.BeginTx(ctx, nil)
	if err != nil {
		return nil, yterrors.Err("could not start the transaction preparing the output static table", err)
	}
	defer func() { _ = tx.Abort() }()
	outputTable, created, err := e.ensureOutput(ctx, tx, user, robot, parameters.OutputPath)
	if err != nil {
		return nil, err
	}
	if !created {
		operation, readErr := e.operationClusterClient.GetOperationByAlias(ctx, exportOperationAlias(outputTable.ID), &yt.GetOperationOptions{
			Attributes:     existingExportAttributes,
			IncludeRuntime: ptr.Bool(true),
		})
		if readErr == nil {
			return e.existingExportResponse(operation, user, robot, parameters)
		}
		// A failed lookup is handled as no operation; without an archive, an unknown alias is a resolve error.
		if !yterrors.ContainsMessageRE(readErr, unknownOperationAliasPattern) && !yterrors.ContainsResolveError(readErr) {
			e.logger.Error("Failed to read the operation with the alias of the output table",
				log.String("operation_alias", exportOperationAlias(outputTable.ID)),
				log.String("user", user),
				log.Error(readErr))
		}
		if err := e.checkOutputTableForExport(outputTable); err != nil {
			return nil, yterrors.Err("the table at output_path does not satisfy the export requirements", err)
		}
	}
	if err := e.authorizeExport(ctx, user, callerService, &parameters); err != nil {
		return nil, err
	}
	inputTables, end, err := e.selectAccessLogInputTables(ctx, parameters.Cluster, parameters.Begin, parameters.End)
	if err != nil {
		return nil, err
	}
	inputTablePaths := inputTablePathsLimitedToPathKeyRange(inputTables, parameters.Path)
	effectiveParameters := EffectiveParameters{ExportParameters: parameters, EffectiveEnd: end}
	annotations := map[string]any{
		exportAnnotationKey: exportAnnotation{
			Owner:               user,
			EffectiveParameters: effectiveParameters,
		},
	}
	poolTree := e.config.PoolTree
	if poolTree == "" {
		// Use the same default tree for permission checks and the operation spec.
		if err := e.operationClusterClient.GetNode(ctx, ypath.Path("//sys/pool_trees/@default_tree"), &poolTree, nil); err != nil {
			return nil, yterrors.Err("default access log export pool tree is unavailable", err)
		}
		if poolTree == "" {
			return nil, yterrors.Err("default pool tree is not configured on the operation cluster")
		}
	}
	if err := e.checkPool(ctx, user, robot, parameters.Pool, poolTree); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, yterrors.Err("could not commit the transaction preparing the output static table", err)
	}
	mapReduceClient := mapreduce.New(e.operationClusterClient, mapreduce.WithContext(ctx))

	mapper := &AccessLogExportMapper{
		Path:         parameters.Path,
		Begin:        parameters.Begin,
		End:          effectiveParameters.EffectiveEnd,
		LogsTimezone: e.config.LogsTimezone,
		Filters: ExportFilters{
			UserRegex:   parameters.UserRegex,
			PathRegex:   parameters.PathRegex,
			MethodGroup: parameters.MethodGroup,
			Scope:       parameters.Scope,
			UserType:    parameters.UserType,
		},
	}
	if parameters.Metadata != nil {
		mapper.Filters.MetadataSet = true
		mapper.Filters.Metadata = *parameters.Metadata
	}
	operationSpec := spec.Map()
	operationSpec.Title = fmt.Sprintf("Export access log of %s for %s [%d, %d)", parameters.Cluster, user, parameters.Begin, effectiveParameters.EffectiveEnd)
	operationSpec.Annotations = annotations
	operationSpec.Alias = exportOperationAlias(outputTable.ID)
	operationSpec.Pool = parameters.Pool
	operationSpec.PoolTrees = []string{poolTree}
	operationSpec.InputTablePaths = inputTablePaths
	operationSpec.OutputTablePaths = []ypath.YPath{outputTable.ID.YPath()}
	operationSpec.Ordered = true
	operation, err := mapReduceClient.Map(mapper, operationSpec)
	if err != nil {
		// An operation with the alias may exist despite the error, for example from a concurrent request to the same table.
		startedOperation, readErr := e.operationClusterClient.GetOperationByAlias(ctx, exportOperationAlias(outputTable.ID), &yt.GetOperationOptions{
			Attributes:     existingExportAttributes,
			IncludeRuntime: ptr.Bool(true),
		})
		if readErr == nil {
			return e.existingExportResponse(startedOperation, user, robot, parameters)
		}
		// The scheduler resolves the alias before it creates the node of the operation.
		if yterrors.ContainsErrorCode(readErr, yterrors.CodeAPINoSuchOperation) {
			return nil, yterrors.Err("an access log export to output_path is starting, retry the request", err, readErr)
		}
		return nil, yterrors.Err("could not start the access log export operation", err)
	}

	e.logger.Info("Access log export started",
		log.String("operation_id", operation.ID().String()),
		log.String("user", user),
		log.String("caller_service", callerService),
		log.Any("effective_parameters", effectiveParameters))
	response := e.exportResponse(operation.ID(), &effectiveParameters)
	return &response, nil
}

func (e *AccessLogExporter) exportResponse(operationID yt.OperationID, parameters *EffectiveParameters) ExportAccessLogResponse {
	return ExportAccessLogResponse{
		ExportID:      formatExportID(e.config.OperationCluster, operationID),
		OutputCluster: e.config.OperationCluster,
		OutputPath:    parameters.OutputPath,
		Path:          parameters.Path,
		Begin:         parameters.Begin,
		End:           parameters.EffectiveEnd,
	}
}

func exportOperationAlias(outputID yt.NodeID) string {
	return exportOperationAliasPrefix + outputID.String()
}

func (e *AccessLogExporter) existingExportResponse(operation *yt.OperationStatus, user, robot string, requested ExportParameters) (*ExportAccessLogResponse, error) {
	annotation, err := readExportAnnotation(operation)
	if err != nil {
		return nil, yterrors.Err("the output_path is used by an operation that is not an access log export",
			yterrors.Attr("operation_id", operation.ID.String()),
			err)
	}
	if operation.AuthenticatedUser != robot {
		return nil, yterrors.Err("the output_path is used by an operation that the service robot has not started",
			yterrors.Attr("operation_id", operation.ID.String()))
	}
	// The export of another user is not named.
	if annotation.Owner != user {
		return nil, yterrors.Err("the output_path is used by an access log export of another user")
	}
	response := e.exportResponse(operation.ID, &annotation.EffectiveParameters)
	if !sameExportParameters(requested, annotation.EffectiveParameters.ExportParameters) {
		return nil, yterrors.Err("the output_path is used by an access log export with other parameters",
			yterrors.Attr("export_id", response.ExportID))
	}
	return &response, nil
}

// sameExportParameters compares the parameters as JSON, in which a missing selector equals an empty one.
func sameExportParameters(left, right ExportParameters) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func exportIDFromRequest(r *http.Request) (string, error) {
	exportID := chi.URLParam(r, "export_id")
	if r.URL.RawPath == "" {
		return exportID, nil
	}
	exportID, err := url.PathUnescape(exportID)
	if err != nil {
		return "", yterrors.Err("invalid export_id", err)
	}
	return exportID, nil
}

func (e *AccessLogExporter) getOwnedOperation(r *http.Request) (*yt.OperationStatus, *exportAnnotation, error) {
	exportID, err := exportIDFromRequest(r)
	if err != nil {
		return nil, nil, err
	}
	operation, annotation, err := e.getExportOperation(r.Context(), exportID)
	if err != nil {
		return nil, nil, err
	}
	// The user is authenticated on the source cluster of the export, which is known only from the annotation.
	user, _, err := e.authenticateUser(r, annotation.EffectiveParameters.Cluster)
	if err != nil {
		return nil, nil, err
	}
	if annotation.Owner != user {
		return nil, nil, yterrors.Err("access to the export is denied")
	}
	return operation, annotation, nil
}

func (e *AccessLogExporter) getExportOperation(ctx context.Context, exportID string) (*yt.OperationStatus, *exportAnnotation, error) {
	operationCluster, operationID, err := parseExportID(exportID)
	if err != nil {
		return nil, nil, err
	}
	if operationCluster != e.config.OperationCluster {
		return nil, nil, yterrors.Err("the export_id belongs to another operation cluster",
			yterrors.Attr("export_id", exportID),
			yterrors.Attr("operation_cluster", e.config.OperationCluster))
	}
	return e.readExportOperation(ctx, operationID)
}

// readExportOperation reads the operation on the operation cluster and checks that the service robot has started it.
func (e *AccessLogExporter) readExportOperation(ctx context.Context, operationID yt.OperationID) (*yt.OperationStatus, *exportAnnotation, error) {
	robot, err := e.getRobot(ctx)
	if err != nil {
		return nil, nil, yterrors.Err("could not get the service robot login", err)
	}
	operation, err := e.operationClusterClient.GetOperation(ctx, operationID, &yt.GetOperationOptions{
		Attributes: exportOperationAttributes,
	})
	if err != nil {
		return nil, nil, yterrors.Err("could not read the access log export operation", err)
	}
	annotation, err := readExportAnnotation(operation)
	if err != nil || operation.AuthenticatedUser != robot {
		return nil, nil, yterrors.Err("access to the export is denied")
	}
	return operation, annotation, nil
}

func readExportAnnotation(operation *yt.OperationStatus) (*exportAnnotation, error) {
	rawAnnotation, ok := operation.RuntimeParameters.Annotations[exportAnnotationKey]
	if !ok {
		return nil, fmt.Errorf("operation is not an access log export")
	}
	encodedAnnotation, err := yson.Marshal(rawAnnotation)
	if err != nil {
		return nil, err
	}
	var annotation exportAnnotation
	if err := yson.Unmarshal(encodedAnnotation, &annotation); err != nil {
		return nil, err
	}
	parameters := annotation.EffectiveParameters
	if annotation.Owner == "" || parameters.Cluster == "" || parameters.Path == "" ||
		parameters.OutputPath == "" || parameters.Begin == 0 || parameters.EffectiveEnd == 0 {
		return nil, fmt.Errorf("access log export annotation is incomplete")
	}
	return &annotation, nil
}

func (e *AccessLogExporter) buildExportStatus(operation *yt.OperationStatus, annotation *exportAnnotation) *ExportAccessLogStatusResponse {
	status := &ExportAccessLogStatusResponse{
		ExportAccessLogResponse: e.exportResponse(operation.ID, &annotation.EffectiveParameters),
		State:                   operation.State,
	}
	if counter := operation.BriefProgress.TotalJobCounter; counter != nil {
		status.Jobs = ExportJobCounters{Total: counter.Total, Running: counter.Running, Completed: counter.Completed, Failed: counter.Failed}
	}
	var progress struct {
		EstimatedInputStatistics ExportInputStatistics `yson:"estimated_input_statistics"`
	}
	if yson.Unmarshal(operation.Progress, &progress) == nil {
		status.Input = progress.EstimatedInputStatistics
	}
	var failure string
	switch operation.State {
	case yt.StateFailed:
		failure = "export operation failed"
	case yt.StateAborted:
		failure = "export operation was aborted"
	}
	if failure != "" {
		// yterrors.Err would keep a nil *yterrors.Error as an inner error.
		args := []any{failure}
		if operation.Result != nil && operation.Result.Error != nil {
			args = append(args, operation.Result.Error)
		}
		status.Error = yterrors.Err(args...).(*yterrors.Error)
	}
	return status
}

// TODO(ilyaibraev): In H2, use shared HTTP error classification instead of returning 400 for every error.
func writeExportResponse(w http.ResponseWriter, r *http.Request, response any, err error) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		ytmsvc.WriteError(w, r, err)
		return
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (e *AccessLogExporter) handleStart(w http.ResponseWriter, r *http.Request) {
	response, err := e.start(w, r)
	writeExportResponse(w, r, response, err)
}

func (e *AccessLogExporter) start(w http.ResponseWriter, r *http.Request) (*ExportAccessLogResponse, error) {
	// A JSON body cannot be sent by a cross-site form, so cookie-authenticated requests need a CORS preflight.
	if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		return nil, yterrors.Err("Content-Type must be application/json")
	}
	request, err := decodeExportAccessLogRequest(http.MaxBytesReader(w, r.Body, maxExportRequestBodySize))
	if err != nil {
		return nil, yterrors.Err("invalid export request", err)
	}
	parameters, err := request.Parameters(e.config.DefaultPool)
	if err != nil {
		return nil, err
	}
	states := clusterStates
	if states == nil {
		return nil, yterrors.Err("configured clusters have not been loaded")
	}
	if _, isServed := states[parameters.Cluster]; !isServed {
		return nil, yterrors.Err(fmt.Sprintf("cluster %q is not served", parameters.Cluster))
	}
	user, callerService, err := e.authenticateUser(r, parameters.Cluster)
	if err != nil {
		return nil, err
	}
	return e.startExport(r.Context(), user, callerService, parameters)
}

func (e *AccessLogExporter) getClusterStatus(ctx context.Context, cluster string) ExportClusterStatus {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	status := ExportClusterStatus{Enabled: true, Reason: "snapshot_root"}
	// The directories hold the tables of the whole retention period, so only their node counts are read.
	for _, period := range []string{"1d", "30min"} {
		var count int64
		err := e.operationClusterClient.GetNode(ctx, e.config.SnapshotRoot.Child(cluster).Child(period).Attr("count"), &count, nil)
		if yterrors.ContainsResolveError(err) {
			continue
		}
		if err != nil {
			e.logger.Error("Failed to check access log export input tables", log.String("cluster", cluster), log.Error(err))
			return ExportClusterStatus{Enabled: true, Reason: "yt_error"}
		}
		if count > 0 {
			return ExportClusterStatus{Enabled: true, InputTablesAvailable: true}
		}
		status.Reason = "no_input_tables"
	}
	return status
}

func (e *AccessLogExporter) handleStatus(w http.ResponseWriter, r *http.Request) {
	operation, annotation, err := e.getOwnedOperation(r)
	if err != nil {
		writeExportResponse(w, r, nil, err)
		return
	}
	writeExportResponse(w, r, e.buildExportStatus(operation, annotation), nil)
}

func (e *AccessLogExporter) handleAbort(w http.ResponseWriter, r *http.Request) {
	operation, annotation, err := e.getOwnedOperation(r)
	if err != nil {
		writeExportResponse(w, r, nil, err)
		return
	}
	if !operation.State.IsFinished() {
		message := "aborted by the owner of the access log export"
		abortErr := e.operationClusterClient.AbortOperation(r.Context(), operation.ID, &yt.AbortOperationOptions{AbortMessage: &message})
		// AbortOperation returns after the operation finishes; a concurrent finish is also visible on the re-read.
		operation, annotation, err = e.readExportOperation(r.Context(), operation.ID)
		if err != nil {
			if abortErr != nil {
				err = yterrors.Err("could not abort the access log export operation", abortErr, err)
			} else {
				err = yterrors.Err("abort of the access log export operation was requested, but its final state could not be read", err)
			}
			writeExportResponse(w, r, nil, err)
			return
		}
		if abortErr != nil && !operation.State.IsFinished() {
			writeExportResponse(w, r, nil, yterrors.Err("could not abort the access log export operation", abortErr))
			return
		}
	}
	writeExportResponse(w, r, e.buildExportStatus(operation, annotation), nil)
}

func withExportRequestTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), exportRequestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (e *AccessLogExporter) routes(r chi.Router) {
	r = r.With(withExportRequestTimeout)
	r.Post("/{call:export(-|_)access(-|_)log}", e.handleStart)
	r.Get("/{call:export(-|_)access(-|_)log}/{export_id}", e.handleStatus)
	r.Delete("/{call:export(-|_)access(-|_)log}/{export_id}", e.handleAbort)
}
