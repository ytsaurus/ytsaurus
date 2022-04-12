package fetcher

import (
	"time"
)

type Resolver struct {
	// Port of Url(s)
	Port int `yson:"port"`

	// Mode2 is activated if 'YPEndpoint' is not empty
	// Mode2 deletes old 'Urls'	and sets 'Urls' to relevant reloved list

	// Urls to fetch
	// -mode1
	Urls []string `yson:"urls,omitempty"`

	// Cluster to get Urls from
	// -mode2
	YPCluster string `yson:"cluster,omitempty"`

	// Endpoint to get Urls
	// -mode2
	YPEndpoint string `yson:"endpoint,omitempty"`
}

type Service struct {
	// Type of the service to take sample from (node)
	ServiceType string `yson:"service_type"`

	// Period of taking samples (5m)
	Period time.Duration `yson:"period"`

	// Probability of taking sample on any particular host (0.0001)
	//
	// Greater then 1 is the same as 1, lesser then 0 is the same as 0
	Probability float64 `yson:"probability"`

	// Path to get profile <url:port/Path> ("ytprof/profile")
	ProfilePath string `yson:"path"`

	// Descriptions of Urls to fetch
	Resolvers []Resolver `yson:"resolvers"`
}

type Config struct {
	// Cluster name ("hume")
	Cluster string `yson:"cluster"`

	// Path to the table ("//home/kristevalex/ytprof")
	TablePath string `yson:"table_path"`

	// Descriptions of what profiles to fetch
	Services []Service `yson:"services"`
}
