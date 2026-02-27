package resource_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/httputil/resource"
)

func TestDir_FileSystemServe(t *testing.T) {
	type testCase struct {
		status  int
		path    string
		content []byte
	}

	// static files are configured in the gotest/ya.make
	cases := []testCase{
		{
			http.StatusOK,
			"/something.txt",
			[]byte("something"),
		},
		{
			http.StatusOK,
			"/something.json",
			[]byte(`{"foo":"bar"}`),
		},
		{
			http.StatusNotFound,
			"/testdata/",
			nil,
		},
		{
			http.StatusNotFound,
			"/testdata/fake",
			nil,
		},
		{
			http.StatusNotFound,
			"/fake",
			nil,
		},
	}

	ts := httptest.NewServer(http.FileServer(resource.Dir("/library/go/httputil/resource/gotest/")))
	defer ts.Close()

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			res, err := http.Get(ts.URL + tc.path)
			require.NoError(t, err)
			defer func() { _ = res.Body.Close() }()

			require.Equal(t, tc.status, res.StatusCode)
			b, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			if tc.status == 200 {
				require.Equal(t, tc.content, b)
			}
		})
	}
}

func TestDir_FileSystemServeRoot(t *testing.T) {
	type testCase struct {
		status  int
		path    string
		content []byte
	}

	// static files are configured in the gotest/ya.make
	cases := []testCase{
		{
			http.StatusOK,
			"/library/go/httputil/resource/gotest/something.txt",
			[]byte("something"),
		},
		{
			http.StatusOK,
			"/library/go/httputil/resource/gotest/something.json",
			[]byte(`{"foo":"bar"}`),
		},
		{
			http.StatusNotFound,
			"/library/go/httputil/resource/gotest",
			nil,
		},
		{
			http.StatusNotFound,
			"/library/go/httputil/resource/gotest/",
			nil,
		},
		{
			http.StatusNotFound,
			"/library/go/httputil/resource/gotest/fake",
			nil,
		},
		{
			http.StatusNotFound,
			"/fake",
			nil,
		},
	}

	ts := httptest.NewServer(http.FileServer(resource.Dir("")))
	defer ts.Close()

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			res, err := http.Get(ts.URL + tc.path)
			require.NoError(t, err)
			defer func() { _ = res.Body.Close() }()

			require.Equal(t, tc.status, res.StatusCode)
			b, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			if tc.status == 200 {
				require.Equal(t, tc.content, b)
			}
		})
	}
}
