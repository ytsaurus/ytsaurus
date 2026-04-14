package resource_test

import (
	"net/http"

	"go.ytsaurus.tech/library/go/httputil/resource"
)

func Example_stdlib() {
	uriPath := "/static/"
	http.Handle(uriPath, http.StripPrefix(uriPath, http.FileServer(resource.Dir("/static/"))))
}
