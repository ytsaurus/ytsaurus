package swaggerui_test

import (
	"net/http"

	"go.ytsaurus.tech/library/go/httputil/swaggerui"
)

func Example_withJsonScheme() {
	swaggerScheme := []byte(`
{
  "openapi": "3.0.0",
  "info": {
    "title": "Sample API",
    "version": "0.1.9",
    "description": "Optional multiline or single-line description in [CommonMark](http://commonmark.org/help/) or HTML"
  }
}
`)

	http.Handle("/", http.FileServer(
		swaggerui.NewFileSystem(swaggerui.WithJSONScheme(swaggerScheme)),
	))
}

func Example_withYamlScheme() {
	swaggerScheme := []byte(`
---
openapi: 3.0.0
info:
  title: Sample API
  description: Optional multiline or single-line description in [CommonMark](http://commonmark.org/help/) or HTML.
  version: 0.1.9
`)

	http.Handle("/", http.FileServer(
		swaggerui.NewFileSystem(swaggerui.WithYAMLScheme(swaggerScheme)),
	))
}

func Example_withRemoteScheme() {
	http.Handle("/", http.FileServer(
		swaggerui.NewFileSystem(
			swaggerui.WithRemoteScheme("/my/scheme/path.yaml"),
		),
	))
}
