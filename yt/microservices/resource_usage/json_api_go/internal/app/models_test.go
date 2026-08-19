package app

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWhoamiResponseMarshalling(t *testing.T) {
	tests := []struct {
		name     string
		response WhoamiResponse
		expected string
	}{
		{
			name:     "user",
			response: WhoamiResponse{User: "username"},
			expected: `{"user":"username"}`,
		},
		{
			name:     "service",
			response: WhoamiResponse{Service: "robot-service"},
			expected: `{"service":"robot-service"}`,
		},
		{
			name:     "service acting on behalf of user",
			response: WhoamiResponse{User: "username", Service: "robot-service"},
			expected: `{"user":"username","service":"robot-service"}`,
		},
		{
			name:     "error",
			response: WhoamiResponse{Error: "failed to get user"},
			expected: `{"error":"failed to get user"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.response)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(body))
		})
	}
}
