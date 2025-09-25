//go:build !internal
// +build !internal

package app

type Config struct {
	// Authorization cookie name that will be passed to ythttp client to check login.
	// Used only in opensource.
	AuthCookieName string `yaml:"auth_cookie_name"`

	ConfigBase `yaml:",inline"`
}
