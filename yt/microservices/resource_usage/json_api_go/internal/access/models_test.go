package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthInfoActingLogin(t *testing.T) {
	tests := []struct {
		name     string
		authInfo AuthInfo
		expected string
	}{
		{
			name:     "user only",
			authInfo: AuthInfo{UserLogin: "username"},
			expected: "username",
		},
		{
			name:     "service only",
			authInfo: AuthInfo{ServiceLogin: "robot-service"},
			expected: "robot-service",
		},
		{
			name:     "service acting on behalf of user",
			authInfo: AuthInfo{UserLogin: "username", ServiceLogin: "robot-service"},
			expected: "username",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.authInfo.ActingLogin())
		})
	}
}
