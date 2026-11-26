//go:build !internal
// +build !internal

package docs

import (
	_ "embed"
)

//go:embed external_swagger.json
var SwaggerJSON []byte
