package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
)

func setTestClusterStates(t *testing.T, states map[string]clusterState) {
	t.Helper()
	previous := clusterStates
	clusterStates = states
	t.Cleanup(func() { clusterStates = previous })
}

func TestServedClustersReportsQueryAndExportAvailability(t *testing.T) {
	setTestClusterStates(t, map[string]clusterState{
		"ytserver": {
			VisibleRange: &VisibleTimeRangeResponse{Earliest: 10, Latests: 20},
			Export:       ExportClusterStatus{Enabled: false, InputTablesAvailable: false, Reason: "disabled"},
		},
		"ytserver-export": {
			VisibleRange: nil,
			Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: true},
		},
	})
	result, err := GetServedClustersHandler()(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/served-clusters", nil))
	require.NoError(t, err)
	response := result.(ServedClustersResponse)
	require.Equal(t, []string{"ytserver"}, response.Clusters)
	require.Equal(t, ClusterStatus{
		AccessLog: AccessLogClusterStatus{Available: true},
		Export:    ExportClusterStatus{Enabled: false, InputTablesAvailable: false, Reason: "disabled"},
	}, response.Statuses["ytserver"])
	require.Equal(t, ClusterStatus{
		AccessLog: AccessLogClusterStatus{Available: false, Reason: "chyt_unavailable"},
		Export:    ExportClusterStatus{Enabled: true, InputTablesAvailable: true},
	}, response.Statuses["ytserver-export"])
}

func TestReadinessRequiresCHYTOrExportInputs(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		states map[string]clusterState
		ready  bool
	}{
		{name: "cluster discovery not loaded", states: nil, ready: false},
		{name: "no configured clusters", states: map[string]clusterState{}, ready: false},
		{
			name: "exports disabled with CHYT",
			states: map[string]clusterState{"ytserver": {
				VisibleRange: &VisibleTimeRangeResponse{Earliest: 10, Latests: 20},
				Export:       ExportClusterStatus{Enabled: false, InputTablesAvailable: false, Reason: "disabled"},
			}},
			ready: true,
		},
		{
			name: "exports disabled without CHYT",
			states: map[string]clusterState{"ytserver": {
				VisibleRange: nil,
				Export:       ExportClusterStatus{Enabled: false, InputTablesAvailable: false, Reason: "disabled"},
			}},
			ready: false,
		},
		{
			name: "exports enabled with CHYT and export inputs",
			states: map[string]clusterState{"ytserver": {
				VisibleRange: &VisibleTimeRangeResponse{Earliest: 10, Latests: 20},
				Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: true},
			}},
			ready: true,
		},
		{
			name: "exports enabled with CHYT but no export inputs",
			states: map[string]clusterState{"ytserver": {
				VisibleRange: &VisibleTimeRangeResponse{Earliest: 10, Latests: 20},
				Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: false, Reason: "snapshot_root"},
			}},
			ready: true,
		},
		{
			name: "exports enabled with export inputs but no CHYT",
			states: map[string]clusterState{"ytserver": {
				VisibleRange: nil,
				Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: true},
			}},
			ready: true,
		},
		{
			name: "exports enabled without CHYT or export inputs",
			states: map[string]clusterState{"ytserver": {
				VisibleRange: nil,
				Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: false, Reason: "snapshot_root"},
			}},
			ready: false,
		},
		{
			name: "CHYT and export inputs available on different clusters",
			states: map[string]clusterState{
				"ytserver": {
					VisibleRange: &VisibleTimeRangeResponse{Earliest: 10, Latests: 20},
					Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: false, Reason: "snapshot_root"},
				},
				"ytserver-other": {
					VisibleRange: nil,
					Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: true},
				},
			},
			ready: true,
		},
		{
			name: "one available cluster is enough despite another unavailable cluster",
			states: map[string]clusterState{
				"ytserver": {
					VisibleRange: &VisibleTimeRangeResponse{Earliest: 10, Latests: 20},
					Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: true},
				},
				"ytserver-other": {
					VisibleRange: nil,
					Export:       ExportClusterStatus{Enabled: true, InputTablesAvailable: false, Reason: "snapshot_root"},
				},
			},
			ready: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			setTestClusterStates(t, testCase.states)
			_, err := GetReadinessHandler()(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/readiness", nil))
			require.Equal(t, testCase.ready, err == nil)
		})
	}
}

func TestStartExportClusterValidation(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		states       map[string]clusterState
		errorText    string
		expectWhoAmI bool
	}{
		{name: "cluster discovery not loaded", states: nil, errorText: "configured clusters have not been loaded", expectWhoAmI: false},
		{name: "rejects export for an unconfigured cluster", states: map[string]clusterState{}, errorText: "is not served", expectWhoAmI: false},
		{
			name: "configured cluster without CHYT reaches WhoAmI", states: map[string]clusterState{"ytserver": {VisibleRange: nil}},
			errorText: "could not get the service robot login", expectWhoAmI: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			setTestClusterStates(t, testCase.states)
			exporter := newTestExporter(t)
			whoAmICalled := false
			exporter.operationClusterClient = &exportTestClient{
				whoAmI: func(context.Context, *yt.WhoAmIOptions) (*yt.WhoAmIResult, error) {
					whoAmICalled = true
					return nil, errors.New("robot authentication failed")
				},
			}
			r := httptest.NewRequest(http.MethodPost, "/export-access-log", strings.NewReader(
				`{"cluster":"ytserver","begin":1,"path":"/","output_path":"//tmp/output"}`))
			r.Header.Set("Content-Type", "application/json")
			_, err := exporter.start(httptest.NewRecorder(), r)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), testCase.errorText)
			require.Equal(t, testCase.expectWhoAmI, whoAmICalled)
		})
	}
}
