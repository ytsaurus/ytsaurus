package ytmsvc

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestWriteError(t *testing.T) {
	recorder := httptest.NewRecorder()

	WriteError(recorder, httptest.NewRequest(http.MethodPost, "/", nil), errors.New("invalid request"))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, "application/json", recorder.Result().Header.Get("Content-Type"))
	var response yterrors.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "Invalid request", response.Message)
	require.Equal(t, yterrors.CodeGeneric, response.Code)
}
