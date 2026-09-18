package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.ytsaurus.tech/library/go/core/log/zap"
)

func TestDebugLoginRejectsAnotherSubject(t *testing.T) {
	previousLogger := logger
	logger = &zap.Logger{L: zaptest.NewLogger(t)}
	t.Cleanup(func() { logger = previousLogger })

	handler := createDebugRouter("alice")
	body := `{"cluster":"test-cluster","subject":"bob","paths":["//home/a"]}`
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/check-acl", strings.NewReader(body))
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "cannot check subject")
}
