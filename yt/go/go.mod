module go.ytsaurus.tech/yt/go

require (
	github.com/andybalholm/brotli v1.0.4
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/davecgh/go-spew v1.1.1
	github.com/gofrs/uuid v4.2.0+incompatible
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/tink/go v1.6.0
	github.com/klauspost/compress v1.15.12
	github.com/mitchellh/copystructure v1.2.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/stretchr/testify v1.8.1
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	go.uber.org/atomic v1.10.0
	go.uber.org/goleak v1.2.0
	go.uber.org/zap v1.23.0
	golang.org/x/sync v0.1.0
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
	google.golang.org/protobuf v1.28.1
	go.ytsaurus.tech/library/go/blockcodecs v0.0.0-00010101000000-000000000000
	go.ytsaurus.tech/library/go/core/log v0.0.0-00010101000000-000000000000
	go.ytsaurus.tech/library/go/core/xerrors v0.0.0-00010101000000-000000000000
	go.ytsaurus.tech/library/go/ptr v0.0.0-00010101000000-000000000000
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/crypto v0.5.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	go.ytsaurus.tech/library/go/x/xreflect v0.0.0-00010101000000-000000000000 // indirect
	go.ytsaurus.tech/library/go/x/xruntime v0.0.0-00010101000000-000000000000 // indirect
)

go 1.18

replace github.com/cactus/go-statsd-client => github.com/cactus/go-statsd-client v3.2.1+incompatible

// otel prometheusexporter has transitive dependencies on docker, pin old one, see CONTRIB-2759
replace github.com/docker/docker => github.com/docker/docker v17.12.0-ce-rc1.0.20190822180741-9552f2b2fdde+incompatible

replace go.ytsaurus.tech/library/go/core/xerrors => ../../library/go/core/xerrors

replace go.ytsaurus.tech/library/go/x/xreflect => ../../library/go/x/xreflect

replace go.ytsaurus.tech/library/go/x/xruntime => ../../library/go/x/xruntime

replace go.ytsaurus.tech/library/go/ptr => ../../library/go/ptr

replace go.ytsaurus.tech/library/go/blockcodecs => ../../library/go/blockcodecs

replace go.ytsaurus.tech/library/go/test/testhelpers => ../../library/go/test/testhelpers

replace go.ytsaurus.tech/library/go/core/log => ../../library/go/core/log
