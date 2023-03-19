module go.ytsaurus.tech/library/go/ptr

go 1.18

replace github.com/cactus/go-statsd-client => github.com/cactus/go-statsd-client v3.2.1+incompatible

// otel prometheusexporter has transitive dependencies on docker, pin old one, see CONTRIB-2759
replace github.com/docker/docker => github.com/docker/docker v17.12.0-ce-rc1.0.20190822180741-9552f2b2fdde+incompatible
