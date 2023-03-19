module go.ytsaurus.tech/library/go/core/xerrors

require (
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.1
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
	go.ytsaurus.tech/library/go/test/testhelpers v0.0.0-00010101000000-000000000000
	go.ytsaurus.tech/library/go/x/xreflect v0.0.0-00010101000000-000000000000
	go.ytsaurus.tech/library/go/x/xruntime v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

go 1.18

replace github.com/cactus/go-statsd-client => github.com/cactus/go-statsd-client v3.2.1+incompatible

// otel prometheusexporter has transitive dependencies on docker, pin old one, see CONTRIB-2759
replace github.com/docker/docker => github.com/docker/docker v17.12.0-ce-rc1.0.20190822180741-9552f2b2fdde+incompatible

replace go.ytsaurus.tech/library/go/x/xreflect => ../../x/xreflect

replace go.ytsaurus.tech/library/go/x/xruntime => ../../x/xruntime

replace go.ytsaurus.tech/library/go/test/testhelpers => ../../test/testhelpers
