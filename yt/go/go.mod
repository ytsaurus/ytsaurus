module go.ytsaurus.tech/yt/go

go 1.23.8

require (
	github.com/andybalholm/brotli v1.1.1
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/go-resty/resty/v2 v2.16.3
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v1.0.0
	github.com/google/tink/go v1.7.0
	github.com/klauspost/compress v1.18.0
	github.com/mitchellh/copystructure v1.2.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4/v4 v4.1.22
	github.com/stretchr/testify v1.10.0
	github.com/testcontainers/testcontainers-go v0.33.0
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	go.opentelemetry.io/otel/bridge/opentracing v1.28.0
	go.opentelemetry.io/otel/sdk v1.36.0
	go.opentelemetry.io/otel/trace v1.37.0
	go.uber.org/atomic v1.11.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.0
	go.ytsaurus.tech/library/go/blockcodecs v0.0.3
	go.ytsaurus.tech/library/go/core/buildinfo v0.0.0-20250809130132-fa050e73ac17
	go.ytsaurus.tech/library/go/core/log v0.0.4
	go.ytsaurus.tech/library/go/core/xerrors v0.0.4
	go.ytsaurus.tech/library/go/httputil/middleware/httpmetrics v0.0.2
	go.ytsaurus.tech/library/go/ptr v0.0.2
	golang.org/x/exp v0.0.0-20250305212735-054e65f0b394
	golang.org/x/sync v0.15.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
	google.golang.org/protobuf v1.36.6
)

require (
	dario.cat/mergo v1.0.1 // indirect
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230811130428-ced1acdcaa24 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/OneOfOne/xxhash v1.2.8 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cpuguy83/dockercfg v0.3.1 // indirect
	github.com/creack/pty v1.1.20 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/docker v28.0.4+incompatible // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-chi/chi/v5 v5.1.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.7-0.20211215081658-ee6c8cce8e87 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/sys/user v0.3.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/shirou/gopsutil/v3 v3.24.2 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shoenig/test v1.7.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.35.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.36.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.ytsaurus.tech/library/go/core/metrics v0.0.2 // indirect
	go.ytsaurus.tech/library/go/x/xreflect v0.0.3 // indirect
	go.ytsaurus.tech/library/go/x/xruntime v0.0.4 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250528174236-200df99c418a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250528174236-200df99c418a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
