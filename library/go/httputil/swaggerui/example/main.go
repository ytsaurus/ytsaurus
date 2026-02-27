package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/library/go/core/resource"
	"go.ytsaurus.tech/library/go/httputil/headers"
	"go.ytsaurus.tech/library/go/httputil/swaggerui"
)

func main() {
	var (
		addr    string
		useYaml bool
	)
	flag.StringVar(&addr, "addr", ":3000", "addr to serve on")
	flag.BoolVar(&useYaml, "yaml", false, "use yaml definition")
	flag.Parse()

	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headers.ContentTypeKey, headers.TypeTextHTML.String())
		_, _ = fmt.Fprint(w, `Hi there, check our cool documentation <a href="/swagger/">here</a>!`)
	})

	r.Route("/swagger/", func(root chi.Router) {
		var opts []swaggerui.Option
		if useYaml {
			opts = []swaggerui.Option{
				swaggerui.WithYAMLScheme(
					resource.MustGet("swagger.yaml"),
				),
			}
		} else {
			opts = []swaggerui.Option{
				swaggerui.WithJSONScheme(
					resource.MustGet("swagger.json"),
				),
			}
		}

		fs := http.StripPrefix(
			"/swagger/",
			http.FileServer(
				swaggerui.NewFileSystem(opts...),
			),
		)

		root.Get("/*", fs.ServeHTTP)
	})

	err := http.ListenAndServe(addr, r)
	if err != nil {
		panic(err)
	}
}
