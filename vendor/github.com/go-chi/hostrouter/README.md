# HostRouter

`hostrouter` is a small Go pkg to let you route traffic to different http handlers or routers
based on the request host. This is useful to mount multiple routers on a single server.

## Basic usage example

```go
//...
func main() {
  r := chi.NewRouter()

  r.Use(middleware.RequestID)
  r.Use(middleware.RealIP)
  r.Use(middleware.Logger)
  r.Use(middleware.Recoverer)

  hr := hostrouter.New()

  // Requests to api.domain.com
  hr.Map("", apiRouter()) // default
  hr.Map("api.domain.com", apiRouter())

  // Requests to doma.in
  hr.Map("doma.in", shortUrlRouter())

  // Requests to *.doma.in
  hr.Map("*.doma.in", shortUrlRouter())

  // Requests to host that isn't defined above
  hr.Map("*", everythingElseRouter())

  // Mount the host router
  r.Mount("/", hr)

  http.ListenAndServe(":3333", r)
}

// Router for the API service
func apiRouter() chi.Router {
  r := chi.NewRouter()
  r.Get("/", apiIndexHandler)
  // ...
  return r
}

// Router for the Short URL service
func shortUrlRouter() chi.Router {
  r := chi.NewRouter()
  r.Get("/", shortIndexHandler)
  // ...
  return r
}
```
