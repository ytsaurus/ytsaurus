package fetcher

import (
	"time"
)

type Resolver struct {
	// Port of Url(s)
	Port int `yson:"port"`

	// Mode2 is activated if 'YPEndpoint' is not empty
	// Mode3 is activated if 'PodSet' is not empty

	// Urls to fetch
	// -mode1
	Urls []string `yson:"urls,omitempty"`

	// Cluster to get Urls from
	// -mode2 or mode3
	YPCluster string `yson:"cluster,omitempty"`

	// Endpoint to get Urls
	// -mode2
	YPEndpoint string `yson:"endpoint,omitempty"`

	// PodSet to get Urls
	// -mode3
	YPPodSet string `yson:"podset,omitempty"`
}

type Service struct {
	// Type of the service to take sample from (node)
	ServiceType string `yson:"service_type"`

	// Type of the profile (cpu/memory)
	ProfileType string `yson:"profile_type"`

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

	// Fills empty paths in services
	ProfilePath string `yson:"path"`

	// Fills empty types in services
	ProfileType string `yson:"profile_type"`

	// Fills empty periods in services
	Period time.Duration `yson:"period"`

	// Fills empty probabilities in services
	Probability float64 `yson:"probability"`

	// Descriptions of what profiles to fetch
	Services []Service `yson:"services"`
}

type Configs struct {
	// Configs per cluster
	Configs []Config `yson:"configs"`

	// Ports by service type
	Ports map[string]int `yson:"ports,omitempty"`
}

func (cs *Configs) FillInfo() {
	for i := 0; i < len(cs.Configs); i++ {
		if cs.Ports != nil {
			cs.Configs[i].FillPorts(cs.Ports)
		}

		cs.Configs[i].FillInfo()
	}
}

func (c *Config) FillInfo() {
	for i := 0; i < len(c.Services); i++ {
		if c.ProfilePath != "" && c.Services[i].ProfilePath == "" {
			c.Services[i].ProfilePath = c.ProfilePath
		}

		if c.ProfileType != "" && c.Services[i].ProfileType == "" {
			c.Services[i].ProfileType = c.ProfileType
		}

		if c.Probability != 0.0 && c.Services[i].Probability == 0.0 {
			c.Services[i].Probability = c.Probability
		}

		if c.Period != 0 && c.Services[i].Period == 0 {
			c.Services[i].Period = c.Period
		}
	}
}

func (c *Config) FillPorts(ports map[string]int) {
	for i := 0; i < len(c.Services); i++ {
		port, ok := ports[c.Services[i].ServiceType]
		if !ok {
			continue
		}

		for j := 0; j < len(c.Services[i].Resolvers); j++ {
			if c.Services[i].Resolvers[j].Port != 0 {
				continue
			}

			c.Services[i].Resolvers[j].Port = port
		}
	}
}
