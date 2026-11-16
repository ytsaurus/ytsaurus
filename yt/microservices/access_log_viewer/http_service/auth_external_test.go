//go:build !internal
// +build !internal

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestExportStatusAuthenticationOnSourceCluster(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		cookie             string
		proxyStatus        int
		proxyBody          string
		proxyError         string
		expectedStatus     int
		expectedCode       yterrors.ErrorCode
		expectProxyRequest bool
	}{
		{
			name: "valid cookie", cookie: "source-cluster-cookie", proxyStatus: http.StatusOK, proxyBody: `{"login":"alice"}`,
			proxyError: "", expectedStatus: http.StatusOK, expectedCode: 0, expectProxyRequest: true,
		},
		{
			name: "missing cookie", cookie: "", proxyStatus: http.StatusOK, proxyBody: `{"login":"alice"}`,
			expectedStatus: http.StatusBadRequest, expectedCode: yterrors.CodeInvalidCredentials, expectProxyRequest: false,
		},
		{
			name: "invalid cookie", cookie: "invalid-cookie", proxyStatus: http.StatusUnauthorized,
			proxyError:     `{"code":111,"message":"authentication diagnostic"}`,
			expectedStatus: http.StatusBadRequest, expectedCode: yterrors.CodeInvalidCredentials, expectProxyRequest: true,
		},
		{
			name: "proxy returns a non-authentication error", cookie: "source-cluster-cookie", proxyStatus: http.StatusBadRequest,
			proxyError:     `{"code":1,"message":"backend diagnostic"}`,
			expectedStatus: http.StatusBadRequest, expectedCode: yterrors.CodeGeneric, expectProxyRequest: true,
		},
		{
			name: "empty login", cookie: "source-cluster-cookie", proxyStatus: http.StatusOK, proxyBody: `{"login":""}`,
			proxyError: "", expectedStatus: http.StatusBadRequest, expectedCode: yterrors.CodeGeneric, expectProxyRequest: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			sourceCluster := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case requests <- r:
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				if testCase.proxyError != "" {
					w.Header().Set("X-YT-Error", testCase.proxyError)
				}
				w.WriteHeader(testCase.proxyStatus)
				_, _ = w.Write([]byte(testCase.proxyBody))
			}))
			defer sourceCluster.Close()
			exporter := newTestExporter(t)
			exporter.authorizer.config = &AuthorizerConfig{authCookieName: "YTCypressCookie"}
			exporter.robot = "robot"
			operationID := yt.OperationID(guid.New())
			exporter.operationClusterClient = &exportTestClient{getOperation: func(_ context.Context, id yt.OperationID, _ *yt.GetOperationOptions) (*yt.OperationStatus, error) {
				return newTestExportOperation(t, id, yt.StateCompleted, sourceCluster.URL)
			}}
			router := newTestRouter(exporter.authorizer, exporter)
			request := httptest.NewRequest(http.MethodGet, "/export-access-log/"+formatExportID("ytserver", operationID), nil)
			if testCase.cookie != "" {
				request.AddCookie(&http.Cookie{Name: "YTCypressCookie", Value: testCase.cookie})
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			require.Equal(t, testCase.expectedStatus, recorder.Code, recorder.Body.String())
			if testCase.proxyError != "" {
				require.Contains(t, recorder.Body.String(), "diagnostic")
			}
			if testCase.expectedStatus == http.StatusBadRequest {
				var responseError yterrors.Error
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &responseError))
				require.Equal(t, testCase.expectedCode, responseError.Code)
			}
			select {
			case authRequest := <-requests:
				require.True(t, testCase.expectProxyRequest)
				require.Equal(t, "/auth/whoami", authRequest.URL.Path)
				cookie, err := authRequest.Cookie("YTCypressCookie")
				require.NoError(t, err)
				require.Equal(t, testCase.cookie, cookie.Value)
			default:
				require.False(t, testCase.expectProxyRequest, "the source cluster did not receive the authentication request")
			}
		})
	}
}
