module go.ytsaurus.tech/library/go/blockcodecs

require (
	github.com/andybalholm/brotli v1.0.4
	github.com/golang/snappy v0.0.4
	github.com/klauspost/compress v1.15.12
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/stretchr/testify v1.8.1
	go.uber.org/atomic v1.10.0
	go.uber.org/goleak v1.2.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/frankban/quicktest v1.14.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/tools v0.1.12 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

go 1.18

replace github.com/cactus/go-statsd-client => github.com/cactus/go-statsd-client v3.2.1+incompatible

// otel prometheusexporter has transitive dependencies on docker, pin old one, see CONTRIB-2759
replace github.com/docker/docker => github.com/docker/docker v17.12.0-ce-rc1.0.20190822180741-9552f2b2fdde+incompatible
