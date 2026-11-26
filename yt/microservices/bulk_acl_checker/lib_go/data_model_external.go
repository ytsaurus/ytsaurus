//go:build !internal
// +build !internal

package lib

type WhoamiResponse struct {
	User string `json:"user"`
}
