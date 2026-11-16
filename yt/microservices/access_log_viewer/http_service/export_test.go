package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func validExportRequest() ExportAccessLogRequest {
	return ExportAccessLogRequest{
		Cluster:    "ytserver",
		Begin:      100.7,
		OutputPath: "//tmp/output",
		Path:       "//home/project",
	}
}

func TestExportRequestValidation(t *testing.T) {
	request := validExportRequest()
	end := 200.2
	request.End = &end
	parameters, err := request.Parameters("default-pool")
	require.NoError(t, err)
	require.Equal(t, int64(100), parameters.Begin)
	require.Equal(t, int64(200), parameters.End)
	require.Equal(t, "default-pool", parameters.Pool)
	_, err = request.Parameters("")
	require.ErrorContains(t, err, "pool is required")

	pool := "my-pool"
	request.Pool = &pool
	parameters, err = request.Parameters("default-pool")
	require.NoError(t, err)
	require.Equal(t, "my-pool", parameters.Pool)
	pool = "Admin:pool"
	_, err = request.Parameters("default-pool")
	require.NoError(t, err)
	pool = ""
	_, err = request.Parameters("default-pool")
	require.ErrorContains(t, err, "pool must not be empty")

	for _, path := range []string{"/", "//home", `//home/a\/b`, `//home/\@acl`} {
		request := validExportRequest()
		request.Path = path
		_, err := request.Parameters("default-pool")
		require.NoError(t, err, "path: %q", path)
	}
	for _, path := range []string{"", "//", "//home/", "#1-2-3-4", "//home/@acl", "//home&", `//home/\x41`, "<append=%true>//home"} {
		request := validExportRequest()
		request.Path = path
		_, err := request.Parameters("default-pool")
		require.Error(t, err, "path: %q", path)
	}
	for _, outputPath := range []string{"", "/", "//tmp/output/", "//tmp/output/@schema", `//tmp/\x41`, "<append=%true>//tmp/output"} {
		request := validExportRequest()
		request.OutputPath = outputPath
		_, err := request.Parameters("default-pool")
		require.Error(t, err, "output_path: %q", outputPath)
		var pathError *yterrors.Error
		require.ErrorAs(t, err, &pathError)
		if outputPath == "/" {
			require.Equal(t, "The output_path must not be the Cypress root", pathError.Message)
		} else {
			require.Equal(t, "The output_path must be an absolute Cypress path of a table in the canonical YPath escaping", pathError.Message, "output_path: %q", outputPath)
		}
	}

	invalidRequests := map[string]func(*ExportAccessLogRequest){
		"missing cluster":                      func(r *ExportAccessLogRequest) { r.Cluster = "" },
		"begin below one":                      func(r *ExportAccessLogRequest) { r.Begin = 0.5 },
		"end not after begin after truncation": func(r *ExportAccessLogRequest) { end := 100.9; r.End = &end },
		"invalid user regex":                   func(r *ExportAccessLogRequest) { r.UserRegex = "(" },
		"invalid path regex":                   func(r *ExportAccessLogRequest) { r.PathRegex = "[" },
	}
	for name, modify := range invalidRequests {
		request := validExportRequest()
		modify(&request)
		_, err := request.Parameters("default-pool")
		require.Error(t, err, name)
	}
}

func TestNormalizeTimestampInt64Bounds(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		value      float64
		expected   int64
		outOfRange bool
	}{
		{name: "minimum int64", value: math.MinInt64, expected: math.MinInt64, outOfRange: false},
		{name: "largest float64 below int64 upper bound", value: math.Nextafter(1<<63, 0), expected: math.MaxInt64 - 1023, outOfRange: false},
		{name: "below int64 minimum", value: math.Nextafter(math.MinInt64, math.Inf(-1)), outOfRange: true},
		{name: "MaxInt64 rounded up to float64 is out of range", value: float64(math.MaxInt64), outOfRange: true},
		{name: "positive overflow", value: 1e19, outOfRange: true},
		{name: "negative overflow", value: -1e19, outOfRange: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			timestamp, err := normalizeTimestamp(testCase.value)
			if testCase.outOfRange {
				require.EqualError(t, err, "value must be within the int64 Unix timestamp range")
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expected, timestamp)
		})
	}
}

func TestDecodeExportAccessLogRequest(t *testing.T) {
	request, err := decodeExportAccessLogRequest(strings.NewReader(
		`{"cluster": "ytserver", "begin": 1.5, "output_path": "//tmp/output", "path": "/", "strict": false}` + "\n"))
	require.NoError(t, err)
	require.Equal(t, "ytserver", request.Cluster)
	require.Equal(t, 1.5, request.Begin)

	for _, body := range []string{
		`{"cluster": "ytserver", "field_selector": {}}`,
		`{"cluster": "ytserver"} {}`,
		`{"cluster": "ytserver"} x`,
		`{"cluster": "ytserver"}]`,
		`{"begin": "1"}`,
	} {
		_, err := decodeExportAccessLogRequest(strings.NewReader(body))
		require.Error(t, err, "body: %s", body)
	}
}

func TestExportRequestBodyLimit(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		body      string
		errorText string
	}{
		{name: "malformed JSON", body: "{", errorText: "unexpected EOF"},
		{name: "body at size limit reaches field validation", body: strings.Repeat(" ", maxExportRequestBodySize-2) + "{}", errorText: "cluster is required"},
		{name: "body over size limit is rejected before field validation", body: strings.Repeat(" ", maxExportRequestBodySize-1) + "{}", errorText: "request body too large"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			r := httptest.NewRequest(http.MethodPost, "/export-access-log", strings.NewReader(testCase.body))
			r.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			exporter.handleStart(response, r)
			require.Equal(t, http.StatusBadRequest, response.Code)
			var publicError yterrors.Error
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &publicError))
			require.Contains(t, strings.ToLower(publicError.Error()), strings.ToLower(testCase.errorText))
		})
	}
}

func TestExportIDRoundTripAndValidation(t *testing.T) {
	operationID := yt.OperationID(guid.New())
	exportID := formatExportID("ytserver", operationID)
	require.Equal(t, "ytserver:"+operationID.String(), exportID)
	cluster, parsedOperationID, err := parseExportID(exportID)
	require.NoError(t, err)
	require.Equal(t, "ytserver", cluster)
	require.Equal(t, operationID, parsedOperationID)

	for _, invalidExportID := range []string{"", "ytserver", ":" + operationID.String(), "ytserver:not-an-id", "localhost:12345:" + operationID.String()} {
		_, _, err := parseExportID(invalidExportID)
		require.Error(t, err, "export_id: %q", invalidExportID)
	}
}

func makeInputTable(t *testing.T, begin string, duration time.Duration) accessLogInputTable {
	intervalBegin, err := time.Parse(time.RFC3339, begin)
	require.NoError(t, err)
	return accessLogInputTable{
		Path:          ypath.Path("//logs/" + begin),
		IntervalBegin: intervalBegin,
		IntervalEnd:   intervalBegin.Add(duration),
	}
}

func TestSelectInputCandidates(t *testing.T) {
	day := makeInputTable(t, "2024-01-01T00:00:00Z", 24*time.Hour)
	firstHalfHour := makeInputTable(t, "2024-01-02T00:00:00Z", 30*time.Minute)
	secondHalfHour := makeInputTable(t, "2024-01-02T00:30:00Z", 30*time.Minute)
	coveredByDay := makeInputTable(t, "2024-01-01T12:00:00Z", 30*time.Minute)
	candidates := []accessLogInputTable{secondHalfHour, coveredByDay, firstHalfHour, day}

	tables, end, err := selectInputCandidates(candidates, day.IntervalBegin.Unix()+60, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []accessLogInputTable{day, firstHalfHour, secondHalfHour}, tables)
	require.Equal(t, secondHalfHour.IntervalEnd.Unix(), end)

	tables, end, err = selectInputCandidates(candidates, firstHalfHour.IntervalBegin.Unix(), firstHalfHour.IntervalEnd.Unix(), 10)
	require.NoError(t, err)
	require.Equal(t, []accessLogInputTable{firstHalfHour}, tables)
	require.Equal(t, firstHalfHour.IntervalEnd.Unix(), end)

	_, _, err = selectInputCandidates([]accessLogInputTable{day, secondHalfHour}, day.IntervalBegin.Unix(), secondHalfHour.IntervalEnd.Unix(), 10)
	require.ErrorContains(t, err, "hole")

	_, _, err = selectInputCandidates(candidates, day.IntervalBegin.Unix(), 0, 2)
	require.ErrorContains(t, err, "too many input tables")

	_, _, err = selectInputCandidates(candidates, secondHalfHour.IntervalEnd.Unix(), 0, 10)
	require.ErrorContains(t, err, "no access log tables")
}

func TestInputTablePathsLimitedToPathKeyRange(t *testing.T) {
	sortedTable := accessLogInputTable{Path: ypath.Path("#1-2-3-4"), SortedByPath: true}
	unsortedTable := accessLogInputTable{Path: ypath.Path("#5-6-7-8"), SortedByPath: false}
	tables := []accessLogInputTable{sortedTable, unsortedTable}

	subtreeRange := ypath.Interval(ypath.Key("//home/a"), ypath.Key("//home/a0"))
	require.Equal(t, []ypath.YPath{
		&ypath.Rich{Path: sortedTable.Path, Ranges: []ypath.Range{subtreeRange}},
		unsortedTable.Path,
	}, inputTablePathsLimitedToPathKeyRange(tables, "//home/a"))
	// The export of the whole Cypress also reads rows with object IDs and empty paths.
	require.Equal(t, []ypath.YPath{sortedTable.Path, unsortedTable.Path}, inputTablePathsLimitedToPathKeyRange(tables, "/"))
}

func TestIsCanonicalCypressPath(t *testing.T) {
	for _, testCase := range []struct {
		path     string
		expected bool
	}{
		{path: "/", expected: true},
		{path: "//home", expected: true},
		{path: `//home/b\/c/\*/a\@b/\&/\\`, expected: true},
		{path: `//home/\x00`, expected: true},
		{path: `//home/b\x2Fc`, expected: false},
		{path: `//home/\x61`, expected: false},
		{path: "", expected: false},
		{path: "//", expected: false},
		{path: "home", expected: false},
		{path: "#1-2-3-4", expected: false},
		{path: "//home/", expected: false},
		{path: "///home", expected: false},
		{path: "//home//table", expected: false},
		{path: "//home/@acl", expected: false},
		{path: "//home&", expected: false},
		{path: "//home/*", expected: false},
		{path: "//home[1:2]", expected: false},
		{path: "//home{a}", expected: false},
		{path: `<append=%true>//home`, expected: false},
		{path: `//home/invalid\`, expected: false},
		{path: `//home/\q`, expected: false},
		{path: `//home/\xzz`, expected: false},
	} {
		require.Equal(t, testCase.expected, isCanonicalCypressPath(testCase.path), "path: %q", testCase.path)
	}
}

func TestIsPathInExportScope(t *testing.T) {
	for _, testCase := range []struct {
		scope    string
		path     string
		expected bool
	}{
		{scope: "/", path: "//home/a", expected: true},
		{scope: "/", path: "/@acl/0", expected: true},
		{scope: "/", path: "#1-2-3-4/@path", expected: true},
		{scope: "/", path: "", expected: true},
		{scope: "/", path: "home", expected: true},
		{scope: "//0", path: "/@acl/0", expected: false},
		{scope: "//a/b", path: "//a/b", expected: true},
		{scope: "//a/b", path: "//a/b/table", expected: true},
		{scope: "//a/b", path: "//a/b/@acl/0", expected: true},
		{scope: "//a/b", path: "//a/b&", expected: true},
		{scope: "//a/b", path: "//a/b&/@target_path", expected: true},
		{scope: "//a/b", path: "//a/bb", expected: false},
		{scope: "//a/b", path: `//a/b\/c`, expected: false},
		{scope: "//a/b", path: `//a/b\&`, expected: false},
		{scope: "//a/b", path: "//a", expected: false},
		{scope: "//a/b", path: "#1-2-3-4", expected: false},
		{scope: "//a/b", path: "", expected: false},
		{scope: `//a/b\/c`, path: `//a/b\/c/table`, expected: true},
		{scope: `//a/\@acl`, path: "//a/@acl", expected: false},
		{scope: `//a/b\\`, path: `//a/b\\/c`, expected: true},
		{scope: `//a/b\\`, path: `//a/b\/c`, expected: false},
	} {
		require.Equal(t, testCase.expected, isPathInExportScope(testCase.scope, testCase.path), "scope: %q, path: %q", testCase.scope, testCase.path)
	}
}

func TestExportMapperGobPreservesMetadataFilter(t *testing.T) {
	for _, filters := range []ExportFilters{
		{MetadataSet: false, Metadata: false},
		{MetadataSet: true, Metadata: false},
		{MetadataSet: true, Metadata: true},
	} {
		var encoded bytes.Buffer
		require.NoError(t, gob.NewEncoder(&encoded).Encode(&AccessLogExportMapper{Filters: filters}))
		var mapper AccessLogExportMapper
		require.NoError(t, gob.NewDecoder(&encoded).Decode(&mapper))
		require.Equal(t, filters, mapper.Filters)
	}
}

type exportTestClient struct {
	yt.Client
	getNode         func(context.Context, ypath.YPath, any, *yt.GetNodeOptions) error
	listNode        func(context.Context, ypath.YPath, any, *yt.ListNodeOptions) error
	checkPermission func(context.Context, string, yt.Permission, ypath.YPath, *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error)
	createNode      func(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error)
	whoAmI          func(context.Context, *yt.WhoAmIOptions) (*yt.WhoAmIResult, error)
	getOperation    func(context.Context, yt.OperationID, *yt.GetOperationOptions) (*yt.OperationStatus, error)
	abortOperation  func(context.Context, yt.OperationID, *yt.AbortOperationOptions) error
}

func (c *exportTestClient) GetNode(ctx context.Context, path ypath.YPath, value any, opts *yt.GetNodeOptions) error {
	return c.getNode(ctx, path, value, opts)
}

func (c *exportTestClient) ListNode(ctx context.Context, path ypath.YPath, value any, opts *yt.ListNodeOptions) error {
	return c.listNode(ctx, path, value, opts)
}

func (c *exportTestClient) CheckPermission(ctx context.Context, user string, permission yt.Permission, path ypath.YPath, opts *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
	return c.checkPermission(ctx, user, permission, path, opts)
}

func (c *exportTestClient) BeginTx(context.Context, *yt.StartTxOptions) (yt.Tx, error) {
	return &exportTestTx{client: c, id: yt.TxID(guid.New())}, nil
}

type exportTestTx struct {
	yt.Tx
	client *exportTestClient
	id     yt.TxID
}

func (tx *exportTestTx) ID() yt.TxID {
	return tx.id
}

func (tx *exportTestTx) CreateNode(ctx context.Context, path ypath.YPath, nodeType yt.NodeType, opts *yt.CreateNodeOptions) (yt.NodeID, error) {
	return tx.client.createNode(ctx, path, nodeType, opts)
}

func (tx *exportTestTx) GetNode(ctx context.Context, path ypath.YPath, value any, opts *yt.GetNodeOptions) error {
	return tx.client.getNode(ctx, path, value, opts)
}

func (c *exportTestClient) WhoAmI(ctx context.Context, opts *yt.WhoAmIOptions) (*yt.WhoAmIResult, error) {
	return c.whoAmI(ctx, opts)
}

func (c *exportTestClient) GetOperation(ctx context.Context, id yt.OperationID, opts *yt.GetOperationOptions) (*yt.OperationStatus, error) {
	return c.getOperation(ctx, id, opts)
}

func (c *exportTestClient) AbortOperation(ctx context.Context, id yt.OperationID, opts *yt.AbortOperationOptions) error {
	return c.abortOperation(ctx, id, opts)
}

func decodeTestYSON(t *testing.T, value, result any) error {
	t.Helper()
	encoded, err := yson.Marshal(value)
	require.NoError(t, err)
	return yson.Unmarshal(encoded, result)
}

func allowExportPermission(context.Context, string, yt.Permission, ypath.YPath, *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
	return &yt.CheckPermissionResponse{CheckPermissionResult: yt.CheckPermissionResult{Action: yt.ActionAllow}}, nil
}

func TestExportNestedPoolLookupAndPermissions(t *testing.T) {
	exporter := newTestExporter(t)
	checkedUsers := []string{}
	exporter.operationClusterClient = &exportTestClient{
		getNode: func(_ context.Context, path ypath.YPath, result any, _ *yt.GetNodeOptions) error {
			if path == ypath.Path("//sys/scheduler/orchid/scheduler/pool_trees/export-tree/pools/missing") {
				return yterrors.Err("Pool tree \"export-tree\" has no pool \"missing\"")
			}
			require.Equal(t, ypath.Path("//sys/scheduler/orchid/scheduler/pool_trees/export-tree/pools/child"), path)
			return decodeTestYSON(t, map[string]any{"full_path": "/parent/child", "is_ephemeral": false, "parent": "parent"}, result)
		},
		checkPermission: func(ctx context.Context, user string, permission yt.Permission, path ypath.YPath, opts *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
			require.Equal(t, ypath.Path("//sys/pool_trees/export-tree/parent/child"), path)
			require.Equal(t, yt.PermissionUse, permission)
			checkedUsers = append(checkedUsers, user)
			return allowExportPermission(ctx, user, permission, path, opts)
		},
	}
	require.NoError(t, exporter.checkPool(context.Background(), "alice", "robot", "child", "export-tree"))
	require.Equal(t, []string{"alice", "robot"}, checkedUsers)
	checkedUsers = nil
	exporter.config.DefaultPool = "child"
	require.NoError(t, exporter.checkPool(context.Background(), "alice", "robot", "child", "export-tree"))
	require.Equal(t, []string{"robot"}, checkedUsers)
	err := exporter.checkPool(context.Background(), "alice", "robot", "missing", "export-tree")
	require.ErrorContains(t, err, "could not read the access log export pool from the scheduler orchid")
	require.ErrorContains(t, err, "has no pool")
}

func TestExportPoolWithoutNodeRejected(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		poolInfo  map[string]any
		errorText string
	}{
		{
			name: "ephemeral pool", poolInfo: map[string]any{"full_path": "/parent/robot", "is_ephemeral": true},
			errorText: "ephemeral pools are not supported for access log export",
		},
		{
			name: "root of the pool tree", poolInfo: map[string]any{},
			errorText: "operations cannot start in the root of the pool tree",
		},
	} {
		for _, isDefaultPool := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/isDefaultPool=%v", testCase.name, isDefaultPool), func(t *testing.T) {
				exporter := newTestExporter(t)
				if isDefaultPool {
					exporter.config.DefaultPool = "robot"
				}
				exporter.operationClusterClient = &exportTestClient{
					getNode: func(_ context.Context, _ ypath.YPath, result any, _ *yt.GetNodeOptions) error {
						return decodeTestYSON(t, testCase.poolInfo, result)
					},
					checkPermission: func(context.Context, string, yt.Permission, ypath.YPath, *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
						t.Fatal("permissions must not be checked for a pool without a node")
						return nil, nil
					},
				}
				err := exporter.checkPool(context.Background(), "alice", "robot", "robot", "export-tree")
				require.ErrorContains(t, err, testCase.errorText)
			})
		}
	}
}

func TestExportPoolRemovedBeforePermissionCheck(t *testing.T) {
	exporter := newTestExporter(t)
	exporter.operationClusterClient = &exportTestClient{
		getNode: func(_ context.Context, _ ypath.YPath, result any, _ *yt.GetNodeOptions) error {
			return decodeTestYSON(t, map[string]any{"full_path": "/pool", "is_ephemeral": false}, result)
		},
		checkPermission: func(_ context.Context, user string, _ yt.Permission, path ypath.YPath, _ *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
			require.Equal(t, "alice", user)
			require.Equal(t, ypath.Path("//sys/pool_trees/default/pool"), path)
			return nil, yterrors.Err(yterrors.CodeResolveError, "pool was removed")
		},
	}
	err := exporter.checkPool(context.Background(), "alice", "robot", "pool", "default")
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not check access to the requested pool")
	require.True(t, yterrors.ContainsResolveError(err), "error: %v", err)
}

func TestExportPoolRobotPermissionFailures(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		isDefaultPool bool
		permissionErr error
		orchidErr     error
		errorText     string
	}{
		{
			name: "robot denied use on non-default pool", isDefaultPool: false, orchidErr: nil, permissionErr: nil,
			errorText: "Service robot lacks permissions required for the export",
		},
		{
			name: "robot denied use on default pool", isDefaultPool: true, orchidErr: nil, permissionErr: nil,
			errorText: "Service robot lacks permissions required for the export",
		},
		{
			name: "robot permission check returns an authorization error for non-default pool", isDefaultPool: false, orchidErr: nil,
			permissionErr: yterrors.Err(yterrors.CodeAuthorizationError, "access denied"), errorText: "Could not check service robot permissions for the export",
		},
		{
			name: "robot permission check returns an authorization error for default pool", isDefaultPool: true, orchidErr: nil,
			permissionErr: yterrors.Err(yterrors.CodeAuthorizationError, "access denied"), errorText: "Could not check service robot permissions for the export",
		},
		{
			name: "robot permission check returns context.DeadlineExceeded for non-default pool", isDefaultPool: false, orchidErr: nil,
			permissionErr: context.DeadlineExceeded, errorText: "Could not check service robot permissions for the export",
		},
		{
			name: "robot permission check returns context.DeadlineExceeded for default pool", isDefaultPool: true, orchidErr: nil,
			permissionErr: context.DeadlineExceeded, errorText: "Could not check service robot permissions for the export",
		},
		{
			name: "scheduler orchid read returns an authorization error", isDefaultPool: false,
			orchidErr: yterrors.Err(yterrors.CodeAuthorizationError, "access denied"),
			errorText: "Could not read the access log export pool from the scheduler orchid",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			if testCase.isDefaultPool {
				exporter.config.DefaultPool = "pool"
			}
			exporter.operationClusterClient = &exportTestClient{
				getNode: func(_ context.Context, _ ypath.YPath, result any, _ *yt.GetNodeOptions) error {
					if testCase.orchidErr != nil {
						return testCase.orchidErr
					}
					return decodeTestYSON(t, map[string]any{"full_path": "/pool", "is_ephemeral": false}, result)
				},
				checkPermission: func(ctx context.Context, user string, permission yt.Permission, path ypath.YPath, opts *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
					require.Equal(t, ypath.Path("//sys/pool_trees/default/pool"), path)
					require.Equal(t, yt.PermissionUse, permission)
					if user == "alice" {
						return allowExportPermission(ctx, user, permission, path, opts)
					}
					require.Equal(t, "robot", user)
					return &yt.CheckPermissionResponse{CheckPermissionResult: yt.CheckPermissionResult{Action: yt.ActionDeny}}, testCase.permissionErr
				},
			}
			err := exporter.checkPool(context.Background(), "alice", "robot", "pool", "default")
			require.Error(t, err)
			require.ErrorContains(t, err, strings.ToLower(testCase.errorText))
		})
	}
}

func TestExportFullReadMissingUserReturnsCheckPermissionError(t *testing.T) {
	sourceCluster := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/check_permission" {
			t.Errorf("unexpected source-cluster request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("X-YT-Error", `{"code":900,"message":"No such user"}`)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer sourceCluster.Close()
	exporter := newTestExporter(t)
	err := exporter.authorizeExport(context.Background(), "alice", "", &ExportParameters{Cluster: sourceCluster.URL, Path: "/"})
	require.ErrorContains(t, err, "access log export authorization is unavailable")
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeAuthenticationError), "error: %v", err)
}

func TestExportInputDirectoriesMissingOrEmpty(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		availableDirectory string
	}{
		{name: "both input directories missing", availableDirectory: ""},
		{name: "only an empty 1d directory", availableDirectory: "1d"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			availableDirectory := ypath.Path("//logs/ytserver/" + testCase.availableDirectory)
			exporter.operationClusterClient = &exportTestClient{listNode: func(_ context.Context, path ypath.YPath, _ any, _ *yt.ListNodeOptions) error {
				if path.YPath() == availableDirectory {
					return nil
				}
				return yterrors.Err(yterrors.CodeResolveError, "no such directory")
			}}
			_, _, err := exporter.selectAccessLogInputTables(context.Background(), "ytserver", 1, 0)
			require.Error(t, err)
			// The status check reads only the node counts of the directories.
			exporter.operationClusterClient = &exportTestClient{getNode: func(_ context.Context, path ypath.YPath, count any, _ *yt.GetNodeOptions) error {
				if path.YPath() == availableDirectory.Attr("count") {
					return decodeTestYSON(t, 0, count)
				}
				return yterrors.Err(yterrors.CodeResolveError, "no such directory")
			}}
			status := exporter.getClusterStatus(context.Background(), "ytserver")
			require.True(t, status.Enabled)
			require.False(t, status.InputTablesAvailable)
			if testCase.availableDirectory == "" {
				require.ErrorContains(t, err, "access log export input tables are unavailable")
				require.Equal(t, "snapshot_root", status.Reason)
			} else {
				require.ErrorContains(t, err, "there are no access log tables after begin")
				require.Equal(t, "no_input_tables", status.Reason)
			}
		})
	}
	t.Run("input directory read fails", func(t *testing.T) {
		exporter := newTestExporter(t)
		exporter.operationClusterClient = &exportTestClient{getNode: func(context.Context, ypath.YPath, any, *yt.GetNodeOptions) error {
			return errors.New("backend diagnostic")
		}}
		require.Equal(t, ExportClusterStatus{Enabled: true, InputTablesAvailable: false, Reason: "yt_error"}, exporter.getClusterStatus(context.Background(), "ytserver"))
		exporter.operationClusterClient = &exportTestClient{listNode: func(context.Context, ypath.YPath, any, *yt.ListNodeOptions) error {
			return errors.New("backend diagnostic")
		}}
		_, _, err := exporter.selectAccessLogInputTables(context.Background(), "ytserver", 1, 0)
		require.ErrorContains(t, err, "access log export input tables are unavailable")
		require.ErrorContains(t, err, "backend diagnostic")
		var unavailableError *yterrors.Error
		require.ErrorAs(t, err, &unavailableError)
		require.Len(t, unavailableError.InnerErrors, 1)
		require.Equal(t, "//logs/ytserver/1d", unavailableError.InnerErrors[0].Attributes["path"])
	})
}

func TestExportInputTableCoverageErrors(t *testing.T) {
	begin, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	require.NoError(t, err)
	for _, testCase := range []struct {
		name      string
		tables    []string
		errorText string
	}{
		{name: "overlapping input tables", tables: []string{"2024-01-01T00:00:00", "2024-01-01T00:15:00"}, errorText: "Access log tables overlap without containment"},
		{name: "input tables do not cover requested beginning", tables: []string{"2024-01-01T00:30:00"}, errorText: "Access log interval contains a hole"},
		{name: "input tables do not cover requested end", tables: []string{"2024-01-01T00:00:00"}, errorText: "Access log tables do not cover the requested interval"},
		{name: "gap between input tables", tables: []string{"2024-01-01T00:00:00", "2024-01-01T01:00:00"}, errorText: "Access log interval contains a hole"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			exporter.operationClusterClient = &exportTestClient{listNode: func(_ context.Context, path ypath.YPath, result any, _ *yt.ListNodeOptions) error {
				var tables []accessLogTableListItem
				if path.YPath() == "//logs/ytserver/30min" {
					for _, name := range testCase.tables {
						tables = append(tables, accessLogTableListItem{Name: name, ID: yt.NodeID(guid.New()), Type: yt.NodeTable})
					}
				}
				return decodeTestYSON(t, tables, result)
			}}
			_, _, err := exporter.selectAccessLogInputTables(context.Background(), "ytserver", begin.Unix(), begin.Add(90*time.Minute).Unix())
			require.Error(t, err)
			require.ErrorContains(t, err, strings.ToLower(testCase.errorText))
		})
	}
}

// newEnsureOutputTestClient returns a client in which //tmp has parentID, the project account and parentMedium, and
// the node in it is read with readOutput.
func newEnsureOutputTestClient(t *testing.T, parentID yt.NodeID, parentMedium string, readOutput func(ypath.Path, any) error) *exportTestClient {
	return &exportTestClient{
		getNode: func(_ context.Context, path ypath.YPath, result any, _ *yt.GetNodeOptions) error {
			if path.YPath() == "//tmp/@" {
				return decodeTestYSON(t, map[string]any{
					"id": parentID, "account": "project",
					"effective_inheritable_attributes": map[string]any{"primary_medium": parentMedium},
				}, result)
			}
			return readOutput(path.YPath(), result)
		},
		checkPermission: allowExportPermission,
	}
}

func TestExportEnsureOutput(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		exists        bool
		parentMedium  string
		primaryMedium string
	}{
		{name: "create a table with the inherited medium", exists: false, parentMedium: "ssd", primaryMedium: "ssd"},
		{name: "create a table with the default medium", exists: false, parentMedium: "", primaryMedium: "default"},
		{name: "return the existing table", exists: true, parentMedium: "", primaryMedium: "ssd"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			// A zone other than the flag defaults of both builds.
			exporter.config.LogsTimezone = "Asia/Yekaterinburg"
			parentID, outputID := yt.NodeID(guid.New()), yt.NodeID(guid.New())
			nodePath := parentID.YPath().Child("output")
			client := newEnsureOutputTestClient(t, parentID, testCase.parentMedium, func(path ypath.Path, result any) error {
				require.Equal(t, nodePath.SuppressSymlink().Attrs(), path)
				if !testCase.exists {
					return yterrors.Err(yterrors.CodeResolveError, "no such node")
				}
				return decodeTestYSON(t, map[string]any{
					"id": outputID, "type": "table", "account": "project", "primary_medium": "ssd",
				}, result)
			})
			var checks []string
			client.checkPermission = func(ctx context.Context, user string, permission yt.Permission, path ypath.YPath, opts *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
				checks = append(checks, fmt.Sprintf("%s %s %s", user, permission, path.YPath()))
				return allowExportPermission(ctx, user, permission, path, opts)
			}
			client.createNode = func(_ context.Context, path ypath.YPath, nodeType yt.NodeType, opts *yt.CreateNodeOptions) (yt.NodeID, error) {
				require.False(t, testCase.exists)
				require.Equal(t, nodePath, path)
				require.Equal(t, yt.NodeTable, nodeType)
				require.Equal(t, map[string]any{
					"schema":                accessLogExportOutputSchema,
					"account":               "project",
					"primary_medium":        testCase.primaryMedium,
					exportTimezoneAttribute: "Asia/Yekaterinburg",
				}, opts.Attributes)
				return outputID, nil
			}
			exporter.operationClusterClient = client
			tx, err := client.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			output, created, err := exporter.ensureOutput(context.Background(), tx, "alice", "robot", "//tmp/output")
			require.NoError(t, err)
			require.Equal(t, !testCase.exists, created)
			// The parent is checked first, and the table is checked for the user and the robot.
			require.Equal(t, []string{
				fmt.Sprintf("alice write | modify_children %s", parentID.YPath()),
				fmt.Sprintf("alice write %s", outputID.YPath()),
				"alice use //sys/accounts/project",
				fmt.Sprintf("alice use //sys/media/%s", testCase.primaryMedium),
				fmt.Sprintf("robot write %s", outputID.YPath()),
			}, checks)
			require.Equal(t, outputID, output.ID)
		})
	}
}

func TestExportEnsureOutputRejectsNodeThatIsNotStaticTable(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		nodeType yt.NodeType
		dynamic  bool
	}{
		{name: "map_node", nodeType: yt.NodeMap, dynamic: false},
		{name: "dynamic table", nodeType: yt.NodeTable, dynamic: true},
		{name: "link", nodeType: yt.NodeLink, dynamic: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			client := newEnsureOutputTestClient(t, yt.NodeID(guid.New()), "", func(_ ypath.Path, result any) error {
				return decodeTestYSON(t, map[string]any{
					"id": yt.NodeID(guid.New()), "type": testCase.nodeType, "dynamic": testCase.dynamic,
				}, result)
			})
			exporter.operationClusterClient = client
			tx, err := client.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			_, _, err = exporter.ensureOutput(context.Background(), tx, "alice", "robot", "//tmp/output")
			require.ErrorContains(t, err, "the node at output_path is not a static table")
		})
	}
}

func TestExportEnsureOutputChecksParentAccessFirst(t *testing.T) {
	exporter := newTestExporter(t)
	client := newEnsureOutputTestClient(t, yt.NodeID(guid.New()), "", func(path ypath.Path, _ any) error {
		t.Fatalf("unexpected GetNode: %s", path)
		return nil
	})
	client.checkPermission = func(context.Context, string, yt.Permission, ypath.YPath, *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
		return &yt.CheckPermissionResponse{CheckPermissionResult: yt.CheckPermissionResult{Action: yt.ActionDeny}}, nil
	}
	exporter.operationClusterClient = client
	tx, err := client.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, _, err = exporter.ensureOutput(context.Background(), tx, "alice", "robot", "//tmp/output")
	require.ErrorContains(t, err, "write access to output_path is denied")
	var ytError *yterrors.Error
	require.ErrorAs(t, err, &ytError)
	require.Equal(t, "//tmp", ytError.Attributes["path"])
}

func TestExportOutputErrorsIncludeOriginalError(t *testing.T) {
	for _, stage := range []string{"read parent", "read output", "create output"} {
		for _, testCase := range []struct {
			name string
			err  error
		}{
			{name: "authorization error", err: yterrors.Err("request failed", yterrors.Err(yterrors.CodeAuthorizationError, "access denied"))},
			{name: "deadline exceeded error", err: context.DeadlineExceeded},
			{name: "authentication error", err: yterrors.Err(yterrors.CodeAuthenticationError, "invalid robot credentials")},
			{name: "account limit error", err: yterrors.Err(yterrors.CodeAccountLimitExceeded, "account limit exceeded")},
		} {
			t.Run(stage+"/"+testCase.name, func(t *testing.T) {
				exporter := newTestExporter(t)
				client := newEnsureOutputTestClient(t, yt.NodeID(guid.New()), "", func(ypath.Path, any) error {
					if stage == "read output" {
						return testCase.err
					}
					return yterrors.Err(yterrors.CodeResolveError, "no such node")
				})
				if stage == "read parent" {
					client.getNode = func(context.Context, ypath.YPath, any, *yt.GetNodeOptions) error {
						return testCase.err
					}
				}
				client.createNode = func(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error) {
					require.Equal(t, "create output", stage)
					return yt.NodeID{}, testCase.err
				}
				exporter.operationClusterClient = client
				tx, err := client.BeginTx(context.Background(), nil)
				require.NoError(t, err)
				_, _, err = exporter.ensureOutput(context.Background(), tx, "alice", "robot", "//tmp/output")
				require.Error(t, err)
				response := httptest.NewRecorder()
				writeExportResponse(response, httptest.NewRequest(http.MethodPost, "/export-access-log", nil), nil, err)
				require.Equal(t, http.StatusBadRequest, response.Code)
				require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
				require.Equal(t, "application/json", response.Header().Get("Content-Type"))
				var publicError yterrors.Error
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &publicError))
				require.Equal(t, yterrors.CodeGeneric, publicError.Code)
				require.Contains(t, publicError.Message, "Could not")
				require.Len(t, publicError.InnerErrors, 1)
				expectedError, err := json.Marshal(yterrors.FromError(testCase.err))
				require.NoError(t, err)
				actualError, err := json.Marshal(publicError.InnerErrors[0])
				require.NoError(t, err)
				require.JSONEq(t, string(expectedError), string(actualError))
			})
		}
	}
}

func TestExportOutputAccessChecks(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		deniedUser       string
		deniedPermission yt.Permission
		deniedPath       ypath.Path
		permissionErr    error
		errorText        string
	}{
		{name: "all checks pass", deniedUser: "", deniedPermission: "", deniedPath: "", permissionErr: nil, errorText: ""},
		{
			name: "user denied write on the table", deniedUser: "alice", deniedPermission: yt.PermissionWrite, deniedPath: "",
			permissionErr: nil, errorText: "Write access to output_path is denied",
		},
		{
			name: "user denied use on the account", deniedUser: "alice", deniedPermission: yt.PermissionUse,
			deniedPath: "//sys/accounts/project", permissionErr: nil, errorText: "Use access to the output account is denied",
		},
		{
			name: "user denied use on the primary medium", deniedUser: "alice", deniedPermission: yt.PermissionUse,
			deniedPath: "//sys/media/ssd", permissionErr: nil, errorText: "Use access to the output primary medium is denied",
		},
		{
			name: "user use check on the account returns an error", deniedUser: "alice", deniedPermission: yt.PermissionUse,
			deniedPath: "//sys/accounts/project", permissionErr: errors.New("unavailable"),
			errorText: "Could not check use access to the output account",
		},
		{
			name: "robot denied write on the table", deniedUser: "robot", deniedPermission: yt.PermissionWrite, deniedPath: "",
			permissionErr: nil, errorText: "Service robot lacks permissions required for the export",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			outputID, txID := yt.NodeID(guid.New()), yt.TxID(guid.New())
			var checks []string
			exporter.operationClusterClient = &exportTestClient{
				checkPermission: func(ctx context.Context, user string, permission yt.Permission, path ypath.YPath, opts *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
					checks = append(checks, fmt.Sprintf("%s %s %s", user, permission, path.YPath()))
					if permission == yt.PermissionWrite {
						require.Equal(t, outputID.YPath(), path)
						require.Equal(t, txID, opts.TransactionID)
					}
					deniedPath := testCase.deniedPath
					if deniedPath == "" {
						deniedPath = outputID.YPath()
					}
					if user == testCase.deniedUser && permission == testCase.deniedPermission && path.YPath() == deniedPath {
						return &yt.CheckPermissionResponse{CheckPermissionResult: yt.CheckPermissionResult{Action: yt.ActionDeny}}, testCase.permissionErr
					}
					return allowExportPermission(ctx, user, permission, path, opts)
				},
			}
			output := &outputNodeAttributes{ID: outputID, Account: "project", PrimaryMedium: "ssd"}
			err := exporter.checkOutputAccess(context.Background(), "alice", "robot", output, txID)
			if testCase.errorText != "" {
				require.ErrorContains(t, err, strings.ToLower(testCase.errorText))
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{
				fmt.Sprintf("alice write %s", outputID.YPath()),
				"alice use //sys/accounts/project",
				"alice use //sys/media/ssd",
				fmt.Sprintf("robot write %s", outputID.YPath()),
			}, checks)
		})
	}
}

func TestExportRobotLookupDoesNotBlockOtherRequests(t *testing.T) {
	exporter := newTestExporter(t)
	// Cancellation on return also releases a blocked lookup when the test fails.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	exporter.operationClusterClient = &exportTestClient{whoAmI: func(ctx context.Context, _ *yt.WhoAmIOptions) (*yt.WhoAmIResult, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &yt.WhoAmIResult{Login: "robot"}, nil
	}}
	firstResult := make(chan error, 1)
	go func() { _, err := exporter.getRobot(ctx); firstResult <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("WhoAmI did not start")
	}
	secondResult := make(chan error, 1)
	go func() { _, err := exporter.getRobot(ctx); secondResult <- err }()
	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("robot lookup blocked a concurrent request")
	}
	close(release)
	require.NoError(t, <-firstResult)
	user, err := exporter.getRobot(ctx)
	require.NoError(t, err)
	require.Equal(t, "robot", user)
	require.Equal(t, int32(2), calls.Load())
}

func TestExportStatusAndAbortRoutesAcceptEscapedIDs(t *testing.T) {
	exporter := newTestExporter(t)
	exporter.robot = "robot"
	operationID := yt.OperationID(guid.New())
	exporter.operationClusterClient = &exportTestClient{getOperation: func(_ context.Context, id yt.OperationID, _ *yt.GetOperationOptions) (*yt.OperationStatus, error) {
		require.Equal(t, operationID, id)
		return newTestExportOperation(t, id, yt.StateCompleted, "ytserver")
	}}
	router := newTestRouter(exporter.authorizer, exporter)
	exportID := formatExportID(exporter.config.OperationCluster, operationID)
	startResponse := ExportAccessLogResponse{ExportID: exportID, OutputCluster: "ytserver", OutputPath: "//tmp/output", Path: "/", Begin: 100, End: 200}
	for _, pathID := range []string{exportID, url.QueryEscape(exportID)} {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			for _, endpoint := range []string{"export-access-log", "v1/export_access_log"} {
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, httptest.NewRequest(method, "/"+endpoint+"/"+pathID, nil))
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
				var status ExportAccessLogStatusResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &status))
				require.Equal(t, startResponse, status.ExportAccessLogResponse)
			}
		}
	}
}

func TestExportAbortOfRunningOperation(t *testing.T) {
	abortErr := yterrors.Err(yterrors.CodeGeneric, "abort failed")
	readErr := yterrors.Err(yterrors.CodeGeneric, "proxy unavailable")
	for _, testCase := range []struct {
		name          string
		abortErr      error
		rereadState   yt.OperationState
		rereadErr     error
		expectedState yt.OperationState
		errorMessage  string
		innerMessages []string
	}{
		{
			name: "abort request succeeds", abortErr: nil, rereadState: yt.StateAborted, rereadErr: nil,
			expectedState: yt.StateAborted, errorMessage: "", innerMessages: nil,
		},
		{
			name: "abort succeeds and re-read fails", abortErr: nil, rereadState: "", rereadErr: readErr,
			expectedState: "", errorMessage: "Abort of the access log export operation was requested, but its final state could not be read",
			innerMessages: []string{"Could not read the access log export operation"},
		},
		{
			name: "abort fails while the operation runs", abortErr: abortErr, rereadState: yt.StateRunning, rereadErr: nil,
			expectedState: "", errorMessage: "Could not abort the access log export operation", innerMessages: []string{"Abort failed"},
		},
		{
			name: "abort and re-read fail", abortErr: abortErr, rereadState: "", rereadErr: readErr,
			expectedState: "", errorMessage: "Could not abort the access log export operation",
			innerMessages: []string{"Abort failed", "Could not read the access log export operation"},
		},
		{
			name: "abort fails after the operation completed", abortErr: abortErr, rereadState: yt.StateCompleted, rereadErr: nil,
			expectedState: yt.StateCompleted, errorMessage: "", innerMessages: nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			exporter.robot = "robot"
			operationID := yt.OperationID(guid.New())
			reads, aborted := 0, false
			exporter.operationClusterClient = &exportTestClient{
				getOperation: func(_ context.Context, id yt.OperationID, _ *yt.GetOperationOptions) (*yt.OperationStatus, error) {
					reads++
					if reads == 1 {
						return newTestExportOperation(t, id, yt.StateRunning, "ytserver")
					}
					if testCase.rereadErr != nil {
						return nil, testCase.rereadErr
					}
					return newTestExportOperation(t, id, testCase.rereadState, "ytserver")
				},
				abortOperation: func(_ context.Context, id yt.OperationID, _ *yt.AbortOperationOptions) error {
					require.Equal(t, operationID, id)
					aborted = true
					return testCase.abortErr
				},
			}
			router := newTestRouter(exporter.authorizer, exporter)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/export-access-log/"+formatExportID("ytserver", operationID), nil))
			require.True(t, aborted)
			require.Equal(t, 2, reads)
			if testCase.expectedState != "" {
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
				var status ExportAccessLogStatusResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &status))
				require.Equal(t, testCase.expectedState, status.State)
				return
			}
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			var responseError yterrors.Error
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &responseError))
			require.Equal(t, testCase.errorMessage, responseError.Message)
			var innerMessages []string
			for _, inner := range responseError.InnerErrors {
				innerMessages = append(innerMessages, inner.Message)
			}
			require.Equal(t, testCase.innerMessages, innerMessages)
		})
	}
}

func TestExportUserTypeMethodAndScopeFilters(t *testing.T) {
	require.Equal(t, "(user IN ('root','system','scheduler','table_mount_informer','yt-clickhouse-cache') as is_system_user)", GetSystemUserSelector())
	require.Equal(t, "((startsWith(user, 'robot-') OR startsWith(user, 'zomb-')) as is_robot_user)", GetRobotUserSelector())
	for _, user := range []string{"root", "system", "scheduler", "table_mount_informer", "yt-clickhouse-cache"} {
		require.True(t, matchesUserTypeFilter(UserTypeSelector{"system": true}, &user), "user: %q", user)
		require.False(t, matchesUserTypeFilter(UserTypeSelector{"human": true}, &user), "user: %q", user)
		require.False(t, matchesUserTypeFilter(UserTypeSelector{"robot": true}, &user), "user: %q", user)
	}
	for _, user := range []string{"alice", "guest", "owner", "yt-alice"} {
		require.False(t, matchesUserTypeFilter(UserTypeSelector{"system": true}, &user), "user: %q", user)
	}
	for _, user := range []string{"robot-export", "zomb-export"} {
		require.True(t, matchesUserTypeFilter(UserTypeSelector{"robot": true}, &user), "user: %q", user)
	}
	robot, human, system := "robot-export", "alice", "root"
	require.True(t, matchesUserTypeFilter(nil, nil))
	require.True(t, matchesUserTypeFilter(UserTypeSelector{"human": true}, &human))
	require.False(t, matchesUserTypeFilter(UserTypeSelector{"human": true}, &robot))
	require.True(t, matchesUserTypeFilter(UserTypeSelector{"human": true, "system": true}, &system))
	require.False(t, matchesUserTypeFilter(UserTypeSelector{"robot": true}, nil))

	get, create := Get, Create
	require.True(t, matchesMethodFilter(nil, nil))
	require.True(t, matchesMethodFilter(ActionSelector{"read": true}, &get))
	require.False(t, matchesMethodFilter(ActionSelector{"read": true}, &create))
	require.True(t, matchesMethodFilter(ActionSelector{"Create": true}, &create))
	require.True(t, matchesMethodFilter(ActionSelector{"read": false}, &create))

	table, mapNode, link := "table", "map_node", "link"
	require.True(t, matchesScopeFilter(nil, nil))
	require.True(t, matchesScopeFilter(ScopeSelector{"table": false}, nil))
	require.True(t, matchesScopeFilter(ScopeSelector{"table": false}, &mapNode))
	require.True(t, matchesScopeFilter(ScopeSelector{"directory": true}, &mapNode))
	require.True(t, matchesScopeFilter(ScopeSelector{"directory": true, "table": false}, &mapNode))
	require.False(t, matchesScopeFilter(ScopeSelector{"directory": true}, &table))
	require.True(t, matchesScopeFilter(ScopeSelector{"other": true}, &link))
	require.False(t, matchesScopeFilter(ScopeSelector{"other": true}, &table))
	require.True(t, matchesScopeFilter(ScopeSelector{"other": true, "table": true}, &table))
	require.False(t, matchesScopeFilter(ScopeSelector{"table": true}, nil))
}

func TestUnixSecondsFromInstant(t *testing.T) {
	moscow, err := time.LoadLocation("Europe/Moscow")
	require.NoError(t, err)
	for _, location := range []*time.Location{time.UTC, moscow} {
		actual, err := unixSecondsFromInstant("2024-01-02 15:00:01", location)
		require.NoError(t, err, "zone: %s", location)
		require.Equal(t, time.Date(2024, 1, 2, 15, 0, 1, 0, location).Unix(), actual, "zone: %s", location)
	}
	for _, instant := range []string{"", "#", "yesterday", "2024-01-02T15:00:01", "2024-01-02 15:00:01+03:00"} {
		_, err = unixSecondsFromInstant(instant, moscow)
		require.Error(t, err, "instant: %q", instant)
	}
}

func TestNewAccessLogExporterOptionalPoolAndTree(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		pool    string
		tree    string
		invalid bool
	}{
		{name: "pool and tree omitted", pool: "", tree: "", invalid: false},
		{name: "only pool configured", pool: "export-pool", tree: "", invalid: false},
		{name: "only tree configured", pool: "", tree: "export-tree", invalid: false},
		{name: "pool and tree configured", pool: "export-pool", tree: "export-tree", invalid: false},
		{name: "invalid pool with tree omitted", pool: "parent/child", tree: "", invalid: true},
		{name: "invalid tree with pool omitted", pool: "", tree: "parent/child", invalid: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// The fake client has no handlers, so any request to the operation cluster would panic.
			exporter, err := NewAccessLogExporter(AccessLogExporterConfig{
				OperationCluster:   "ytserver",
				LogsTimezone:       "UTC",
				DefaultPool:        testCase.pool,
				PoolTree:           testCase.tree,
				MaxInputTableCount: 1000,
			}, &exportTestClient{}, nil, nil)
			if testCase.invalid {
				require.ErrorContains(t, err, "must be a scheduler name")
				require.Nil(t, exporter)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNewAccessLogExporterValidatesOperationClusterName(t *testing.T) {
	for _, testCase := range []struct {
		operationCluster string
		valid            bool
	}{
		{operationCluster: "yt-server", valid: true},
		{operationCluster: "YTServer", valid: true},
		{operationCluster: "yt_server", valid: true},
		{operationCluster: "yt.server", valid: true},
		{operationCluster: "localhost:12345", valid: false},
		{operationCluster: "http://ytserver:80", valid: false},
	} {
		exporter, err := NewAccessLogExporter(AccessLogExporterConfig{
			OperationCluster:   testCase.operationCluster,
			LogsTimezone:       "UTC",
			MaxInputTableCount: 1000,
		}, &exportTestClient{}, nil, nil)
		if testCase.valid {
			require.NoError(t, err, "operation cluster: %q", testCase.operationCluster)
			require.NotNil(t, exporter)
		} else {
			require.ErrorContains(t, err, "must be a cluster name", "operation cluster: %q", testCase.operationCluster)
			require.Nil(t, exporter)
		}
	}
}

func newTestExporter(t *testing.T) *AccessLogExporter {
	testLogger, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	require.NoError(t, err)
	logger = testLogger
	authorizer := newAuthorizer(testLogger, &AuthorizerConfig{debugLogin: "alice"})
	exporter, err := NewAccessLogExporter(AccessLogExporterConfig{
		OperationCluster:   "ytserver",
		SnapshotRoot:       "//logs",
		LogsTimezone:       "UTC",
		TokenEnvVariable:   "YT_ACCESS_LOG_VIEWER_TOKEN",
		DefaultPool:        "access-log-export-pool",
		PoolTree:           "default",
		MaxInputTableCount: 1000,
	}, &exportTestClient{}, authorizer, testLogger)
	require.NoError(t, err)
	return exporter
}

func newTestRouter(authorizer *Authorizer, exporter *AccessLogExporter) http.Handler {
	return GetRouterHandler("ytserver", "alias", "//logs", "YT_ACCESS_LOG_VIEWER_TOKEN", "UTC", authorizer, exporter, &ytmsvc.CORSConfig{})
}

func newTestExportAnnotation(cluster string) exportAnnotation {
	return exportAnnotation{
		Owner: "alice",
		EffectiveParameters: EffectiveParameters{
			ExportParameters: ExportParameters{
				Cluster:    cluster,
				Begin:      100,
				End:        200,
				OutputPath: "//tmp/output",
				Path:       "/",
				Pool:       "access-log-export-pool",
			},
			EffectiveEnd: 200,
		},
	}
}

func newTestExportOperation(t *testing.T, id yt.OperationID, state yt.OperationState, cluster string) (*yt.OperationStatus, error) {
	var operation yt.OperationStatus
	err := decodeTestYSON(t, map[string]any{
		"id": id, "state": state, "authenticated_user": "robot",
		"runtime_parameters": map[string]any{"annotations": map[string]any{exportAnnotationKey: newTestExportAnnotation(cluster)}},
	}, &operation)
	return &operation, err
}

func TestReadExportAnnotationRejectsIncompleteAnnotation(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		modify func(annotation *exportAnnotation)
		valid  bool
	}{
		{name: "complete annotation", modify: func(*exportAnnotation) {}, valid: true},
		{name: "missing owner", modify: func(annotation *exportAnnotation) { annotation.Owner = "" }, valid: false},
		{name: "missing cluster", modify: func(annotation *exportAnnotation) { annotation.EffectiveParameters.Cluster = "" }, valid: false},
		{name: "missing path", modify: func(annotation *exportAnnotation) { annotation.EffectiveParameters.Path = "" }, valid: false},
		{name: "missing output_path", modify: func(annotation *exportAnnotation) { annotation.EffectiveParameters.OutputPath = "" }, valid: false},
		{name: "missing begin", modify: func(annotation *exportAnnotation) { annotation.EffectiveParameters.Begin = 0 }, valid: false},
		{name: "missing effective end", modify: func(annotation *exportAnnotation) { annotation.EffectiveParameters.EffectiveEnd = 0 }, valid: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			annotation := newTestExportAnnotation("ytserver")
			testCase.modify(&annotation)
			var operation yt.OperationStatus
			require.NoError(t, decodeTestYSON(t, map[string]any{
				"runtime_parameters": map[string]any{"annotations": map[string]any{exportAnnotationKey: annotation}},
			}, &operation))
			result, err := readExportAnnotation(&operation)
			if !testCase.valid {
				require.ErrorContains(t, err, "access log export annotation is incomplete")
				return
			}
			require.NoError(t, err)
			require.Equal(t, annotation, *result)
		})
	}
}

func TestExistingExportResponse(t *testing.T) {
	operationID := yt.OperationID(guid.New())
	annotation := newTestExportAnnotation("ytserver")
	requested := annotation.EffectiveParameters.ExportParameters
	requestedWithoutEnd := requested
	requestedWithoutEnd.End = 0
	otherUserAnnotation := newTestExportAnnotation("ytserver")
	otherUserAnnotation.Owner = "bob"
	for _, testCase := range []struct {
		name              string
		authenticatedUser string
		annotations       map[string]any
		requested         ExportParameters
		errorMessage      string
		errorAttributes   map[string]any
	}{
		{
			name: "operation without an export annotation", authenticatedUser: "robot", annotations: map[string]any{},
			requested:       requested,
			errorMessage:    "The output_path is used by an operation that is not an access log export",
			errorAttributes: map[string]any{"operation_id": operationID.String()},
		},
		{
			name: "export annotation of an operation that another user has started", authenticatedUser: "mallory",
			annotations: map[string]any{exportAnnotationKey: annotation}, requested: requested,
			errorMessage:    "The output_path is used by an operation that the service robot has not started",
			errorAttributes: map[string]any{"operation_id": operationID.String()},
		},
		{
			name: "export of another user", authenticatedUser: "robot",
			annotations: map[string]any{exportAnnotationKey: otherUserAnnotation}, requested: requested,
			errorMessage: "The output_path is used by an access log export of another user", errorAttributes: nil,
		},
		{
			name: "request without the end of the export", authenticatedUser: "robot",
			annotations: map[string]any{exportAnnotationKey: annotation}, requested: requestedWithoutEnd,
			errorMessage:    "The output_path is used by an access log export with other parameters",
			errorAttributes: map[string]any{"export_id": formatExportID("ytserver", operationID)},
		},
		{
			name: "request with the parameters of the export", authenticatedUser: "robot",
			annotations: map[string]any{exportAnnotationKey: annotation}, requested: requested,
			errorMessage: "", errorAttributes: nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			var operation yt.OperationStatus
			require.NoError(t, decodeTestYSON(t, map[string]any{
				"id": operationID, "state": yt.StateCompleted, "authenticated_user": testCase.authenticatedUser,
				"runtime_parameters": map[string]any{"annotations": testCase.annotations},
			}, &operation))
			response, err := exporter.existingExportResponse(&operation, "alice", "robot", testCase.requested)
			if testCase.errorMessage == "" {
				require.NoError(t, err)
				require.Equal(t, ExportAccessLogResponse{
					ExportID: formatExportID("ytserver", operationID), OutputCluster: "ytserver",
					OutputPath: "//tmp/output", Path: "/", Begin: 100, End: 200,
				}, *response)
				return
			}
			require.Nil(t, response)
			var responseError *yterrors.Error
			require.ErrorAs(t, err, &responseError)
			require.Equal(t, testCase.errorMessage, responseError.Message)
			if testCase.errorAttributes == nil {
				require.Empty(t, responseError.Attributes)
			} else {
				require.Equal(t, testCase.errorAttributes, responseError.Attributes)
			}
		})
	}
}

func TestExportRoutesDisabledWithoutExporter(t *testing.T) {
	testLogger, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	require.NoError(t, err)
	logger = testLogger
	router := newTestRouter(nil, nil)
	for _, endpoint := range []string{"/export-access-log", "/export_access_log", "/v1/export-access-log", "/v1/export_access_log"} {
		for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodDelete} {
			path := endpoint
			if method != http.MethodPost {
				path += "/not-an-export-id"
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
			require.Equal(t, http.StatusNotFound, recorder.Code, "%s %s: %s", method, path, recorder.Body.String())
		}
	}
	for _, path := range []string{"/liveness", "/ready", "/v1/ready", "/metrics"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, recorder.Code, "%s: %s", path, recorder.Body.String())
	}
}

func TestStartExportRejectsInvalidRequests(t *testing.T) {
	setTestClusterStates(t, map[string]clusterState{})
	exporter := newTestExporter(t)
	for _, testCase := range []struct {
		name        string
		contentType string
		body        string
		errorText   string
	}{
		{
			name: "valid fields with text/plain content type", contentType: "text/plain",
			body:      `{"cluster": "ytserver", "begin": 1, "output_path": "//tmp/o", "path": "/"}`,
			errorText: "Content-Type must be application/json",
		},
		{
			name: "unknown field", contentType: "application/json",
			body: `{"unknown": 1}`, errorText: "Invalid export request",
		},
		{
			name: "path with trailing slash", contentType: "application/json",
			body:      `{"cluster": "ytserver", "begin": 1, "output_path": "//tmp/o", "path": "//a/"}`,
			errorText: "Path must be / or an absolute Cypress path",
		},
		{
			name: "cluster that is not served", contentType: "application/json",
			body:      `{"cluster": "unknown", "begin": 1, "output_path": "//tmp/o", "path": "/"}`,
			errorText: "is not served",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/export-access-log", strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", testCase.contentType)
			recorder := httptest.NewRecorder()
			exporter.handleStart(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, "body: %s", testCase.body)
			require.Contains(t, recorder.Body.String(), testCase.errorText)
			require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
		})
	}
}

func TestExportStatusRejectsMalformedOrOtherClusterIDs(t *testing.T) {
	otherClusterExportID := formatExportID("other-cluster", yt.OperationID(guid.New()))
	for _, testCase := range []struct {
		name         string
		exportID     string
		errorMessage string
		attributes   map[string]any
	}{
		{name: "export_id without a cluster", exportID: "not-an-export-id", errorMessage: "Invalid export_id", attributes: nil},
		{
			name: "export_id of another operation cluster", exportID: otherClusterExportID,
			errorMessage: "The export_id belongs to another operation cluster",
			attributes:   map[string]any{"export_id": otherClusterExportID, "operation_cluster": "ytserver"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporter := newTestExporter(t)
			recorder := httptest.NewRecorder()
			newTestRouter(exporter.authorizer, exporter).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/export-access-log/"+testCase.exportID, nil))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			var responseError yterrors.Error
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &responseError))
			require.Equal(t, testCase.errorMessage, responseError.Message)
			for name, value := range testCase.attributes {
				require.Equal(t, value, responseError.Attributes[name], name)
			}
		})
	}
}

func TestExportOperationLookupErrors(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		err       error
		errorText string
		causeText string
	}{
		{name: "scheduler missing operation", err: yterrors.Err(yterrors.CodeNoSuchOperation, "No such operation"), errorText: "Could not read the access log export operation", causeText: "No such operation"},
		{name: "API missing operation", err: yterrors.Err("lookup failed", yterrors.Err(yterrors.CodeAPINoSuchOperation, "No such operation")), errorText: "Could not read the access log export operation", causeText: "No such operation"},
		{name: "backend failure", err: errors.New("proxy unavailable"), errorText: "Could not read the access log export operation", causeText: "Proxy unavailable"},
	} {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			t.Run(method+"/"+testCase.name, func(t *testing.T) {
				exporter := newTestExporter(t)
				exporter.robot = "robot"
				operationID := yt.OperationID(guid.New())
				exporter.operationClusterClient = &exportTestClient{getOperation: func(_ context.Context, id yt.OperationID, _ *yt.GetOperationOptions) (*yt.OperationStatus, error) {
					require.Equal(t, operationID, id)
					return nil, testCase.err
				}}
				router := newTestRouter(exporter.authorizer, exporter)
				request := httptest.NewRequest(method, "/export-access-log/"+formatExportID("ytserver", operationID), nil)
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, request)
				require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
				require.Contains(t, recorder.Body.String(), testCase.errorText)
				require.Contains(t, recorder.Body.String(), testCase.causeText)
			})
		}
	}
}
