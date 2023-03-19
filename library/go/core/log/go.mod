module go.ytsaurus.tech/library/go/core/log

require (
	github.com/google/go-cmp v0.5.9
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
	go.uber.org/zap v1.23.0
	go.ytsaurus.tech/library/go/core/xerrors v0.0.0-00010101000000-000000000000
)

require (
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/goleak v1.2.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	go.ytsaurus.tech/library/go/x/xreflect v0.0.0-00010101000000-000000000000 // indirect
	go.ytsaurus.tech/library/go/x/xruntime v0.0.0-00010101000000-000000000000 // indirect
)

go 1.18

replace github.com/cactus/go-statsd-client => github.com/cactus/go-statsd-client v3.2.1+incompatible

// otel prometheusexporter has transitive dependencies on docker, pin old one, see CONTRIB-2759
replace github.com/docker/docker => github.com/docker/docker v17.12.0-ce-rc1.0.20190822180741-9552f2b2fdde+incompatible

replace go.ytsaurus.tech/library/go/core/xerrors => ../xerrors

replace go.ytsaurus.tech/library/go/x/xreflect => ../../x/xreflect

replace go.ytsaurus.tech/library/go/x/xruntime => ../../x/xruntime

replace go.ytsaurus.tech/library/go/test/testhelpers => ../../test/testhelpers
