package swaggerui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/httputil/headers"
	"go.ytsaurus.tech/library/go/httputil/swaggerui"
)

func TestJsonFileSystem(t *testing.T) {
	scheme := `{"swagger": "2.0"}`
	cases := []struct {
		path     string
		content  string
		contains string
		err      bool
	}{
		{
			path:    "/scheme.json",
			content: scheme,
		},
		{
			path:     "/index.html",
			contains: `schemeURL = "./scheme.json"`,
		},
		{
			path:     "/swagger-ui.css",
			contains: `.swagger-ui`,
		},
		{
			path: "/scheme.yaml",
			err:  true,
		},
		{
			path: "/404",
			err:  true,
		},
	}

	fs := swaggerui.NewFileSystem(swaggerui.WithJSONScheme([]byte(scheme)))

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			f, err := fs.Open(tc.path)
			if tc.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			content, err := io.ReadAll(f)
			require.NoError(t, err)

			switch {
			case len(tc.content) > 0:
				require.Equal(t, tc.content, string(content))
			case len(tc.contains) > 0:
				require.Contains(t, string(content), tc.contains)
			default:
				t.Fatal("unhandled test case condition, please report this to library owner")
			}
		})
	}
}

func TestYamlFileSystem(t *testing.T) {
	scheme := `swagger: 2.0`
	cases := []struct {
		path     string
		content  string
		contains string
		err      bool
	}{
		{
			path:    "/scheme.yaml",
			content: scheme,
		},
		{
			path:     "/index.html",
			contains: `schemeURL = "./scheme.yaml"`,
		},
		{
			path:     "/swagger-ui.css",
			contains: `.swagger-ui`,
		},
		{
			path: "/scheme.json",
			err:  true,
		},
		{
			path: "/404",
			err:  true,
		},
	}

	fs := swaggerui.NewFileSystem(swaggerui.WithYAMLScheme([]byte(scheme)))

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			f, err := fs.Open(tc.path)
			if tc.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			content, err := io.ReadAll(f)
			require.NoError(t, err)

			switch {
			case len(tc.content) > 0:
				require.Equal(t, tc.content, string(content))
			case len(tc.contains) > 0:
				require.Contains(t, string(content), tc.contains)
			default:
				t.Fatal("unhandled test case condition, please report this to library owner")
			}
		})
	}
}

func TestRemoteFileSystem(t *testing.T) {
	cases := []struct {
		path     string
		content  string
		contains string
		err      bool
	}{
		{
			path:     "/index.html",
			contains: `schemeURL = "/my/cool/scheme?kek=\""`,
		},
		{
			path:     "/swagger-ui.css",
			contains: `.swagger-ui`,
		},
		{
			path: "/scheme.yaml",
			err:  true,
		},
		{
			path: "/scheme.json",
			err:  true,
		},
		{
			path: "/404",
			err:  true,
		},
	}

	fs := swaggerui.NewFileSystem(swaggerui.WithRemoteScheme(`/my/cool/scheme?kek="`))

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			f, err := fs.Open(tc.path)
			if tc.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			content, err := io.ReadAll(f)
			require.NoError(t, err)

			switch {
			case len(tc.content) > 0:
				require.Equal(t, tc.content, string(content))
			case len(tc.contains) > 0:
				require.Contains(t, string(content), tc.contains)
			default:
				t.Fatal("unhandled test case condition, please report this to library owner")
			}
		})
	}
}

func TestHttp(t *testing.T) {
	scheme := `{"swagger": "2.0"}`
	cases := []struct {
		path        string
		status      int
		contentType string
		content     string
		contains    string
	}{
		{
			path:        "/scheme.json",
			status:      http.StatusOK,
			contentType: "application/json",
			content:     scheme,
		},
		{
			path:        "/index.html",
			status:      http.StatusOK,
			contentType: "text/html; charset=utf-8",
			contains:    `schemeURL = "./scheme.json"`,
		},
		{
			path:        "/",
			status:      http.StatusOK,
			contentType: "text/html; charset=utf-8",
			contains:    `schemeURL = "./scheme.json"`,
		},
		{
			path:        "/swagger-ui.css",
			status:      http.StatusOK,
			contentType: "text/css; charset=utf-8",
			contains:    `.swagger-ui`,
		},
		{
			path:   "/404",
			status: http.StatusNotFound,
		},
	}

	fs := swaggerui.NewFileSystem(swaggerui.WithJSONScheme([]byte(scheme)))
	ts := httptest.NewServer(http.FileServer(fs))
	defer ts.Close()

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			rsp, err := http.Get(ts.URL + tc.path)
			require.NoError(t, err)
			defer func() { _ = rsp.Body.Close() }()

			require.Equal(t, tc.status, rsp.StatusCode)

			if tc.status != 200 {
				_, _ = io.Copy(io.Discard, rsp.Body)
				return
			}

			if tc.contentType != "" {
				require.Equal(t, tc.contentType, rsp.Header.Get(headers.ContentTypeKey))
			}

			content, err := io.ReadAll(rsp.Body)
			require.NoError(t, err)

			switch {
			case len(tc.content) > 0:
				require.Equal(t, tc.content, string(content))
			case len(tc.contains) > 0:
				require.Contains(t, string(content), tc.contains)
			default:
				t.Fatal("unhandled test case condition, please report this to library owner")
			}
		})
	}
}
