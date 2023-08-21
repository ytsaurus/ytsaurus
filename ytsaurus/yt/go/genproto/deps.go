//go:build tools
// +build tools

package tools

import (
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go

//go:generate ./genproto.sh
