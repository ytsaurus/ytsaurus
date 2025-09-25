//go:build !internal
// +build !internal

package access

type AccessChecker struct {
	AccessCheckerBase
}

type Config struct {
	AuthCookieName string
	ConfigBase
}
