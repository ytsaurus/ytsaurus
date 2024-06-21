package dockercfg

import (
	"encoding/base64"
	"testing"
)

func TestDecodeBase64Auth(t *testing.T) {
	for _, tc := range base64TestCases() {
		t.Run(tc.name, testBase64Case(tc, func() (string, string, error) {
			return DecodeBase64Auth(tc.config)
		}))
	}
}

func TestGetRegistryCredentials(t *testing.T) {
	t.Run("from base64 auth", func(t *testing.T) {
		for _, tc := range base64TestCases() {
			t.Run(tc.name, func(T *testing.T) {
				config := Config{
					AuthConfigs: map[string]AuthConfig{
						"some.domain": tc.config,
					},
				}
				testBase64Case(tc, func() (string, string, error) {
					return config.GetRegistryCredentials("some.domain")
				})
			})
		}
	})
}

type base64TestCase struct {
	name    string
	config  AuthConfig
	expUser string
	expPass string
	expErr  bool
}

func base64TestCases() []base64TestCase {
	cases := []base64TestCase{
		{name: "empty"},
		{name: "not base64", expErr: true, config: AuthConfig{Auth: "not base64"}},
		{name: "invalid format", expErr: true, config: AuthConfig{
			Auth: base64.StdEncoding.EncodeToString([]byte("invalid format")),
		}},
		{name: "happy case", expUser: "user", expPass: "pass", config: AuthConfig{
			Auth: base64.StdEncoding.EncodeToString([]byte("user:pass")),
		}},
	}

	return cases
}

type testAuthFn func() (string, string, error)

func testBase64Case(tc base64TestCase, authFn testAuthFn) func(t *testing.T) {
	return func(t *testing.T) {
		u, p, err := authFn()
		if tc.expErr && err == nil {
			t.Fatal("expected error")
		}

		if u != tc.expUser || p != tc.expPass {
			t.Errorf("decoded username and password do not match, expected user: %s, password: %s, got user: %s, password: %s", tc.expUser, tc.expPass, u, p)
		}
	}
}
