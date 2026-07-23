package httppuller_test

import (
	"net/http"
	"time"

	"go.ytsaurus.tech/library/go/core/metrics/solomon"
	"go.ytsaurus.tech/library/go/yandex/solomon/reporters/puller/httppuller"
	"go.ytsaurus.tech/library/go/yandex/tvm"
)

func ExampleNewHandler() {
	// create metrics registry
	opts := solomon.NewRegistryOpts().
		SetSeparator('_').
		SetPrefix("myprefix")

	reg := solomon.NewRegistry(opts)

	// register new metric
	cnt := reg.Counter("cyclesCount")

	// pass metric to your function and do job
	go func() {
		for {
			cnt.Inc()
			time.Sleep(1 * time.Second)
		}
	}()

	// start HTTP server with handler on /metrics URI
	mux := http.NewServeMux()
	mux.Handle("/metrics", httppuller.NewHandler(reg))

	// Or start
	var tvm tvm.Client
	mux.Handle("/secure_metrics", httppuller.NewHandler(reg, httppuller.WithTVM(tvm)))

	_ = http.ListenAndServe(":80", mux)
}
