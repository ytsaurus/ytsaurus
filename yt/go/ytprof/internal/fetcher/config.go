package fetcher

import (
	"fmt"
	"time"
)

// Resolver describes url to fetch to a way to resolve those urls
//
// Supported resolve modes:
//
//   - mode1 is activated if none of the following are.
//   - mode2 is activated if 'YPEndpoint' is not empty.
//   - mode3 is activated if 'PodSet' is not empty.
type Resolver struct {
	// Port is a port used to fetch profiles.
	Port int `yson:"port"`

	// URLs to fetch.
	//  - mode1.
	URLs []string `yson:"urls,omitempty"`

	// YPCluster to get urls from.
	//  - mode2 or mode3.
	YPCluster string `yson:"cluster,omitempty"`

	// YPEndpoint to get urls.
	//  - mode2.
	YPEndpoint string `yson:"endpoint,omitempty"`

	// YPPodSet to get urls.
	//  - mode3.
	YPPodSet string `yson:"podset,omitempty"`
}

type Service struct {
	// ServiceType is a type of the service to take profile samples from (eg node).
	ServiceType string `yson:"service_type"`

	// ProfileType is a type of the profile to take (eg cpu/memory).
	ProfileType string `yson:"profile_type"`

	// Period is a period of taking samples (eg 5m).
	Period time.Duration `yson:"period"`

	// Probability is a probability of taking sample on any particular host (eg 0.0001).
	//
	// Greater then 1 is the same as 1, lesser then 0 is the same as 0.
	Probability float64 `yson:"probability"`

	// ProfilePath is a suffix the request to get profile.
	// Like 'ProfilePath' here "url:port/ProfilePath" (eg ytprof/profile).
	ProfilePath string `yson:"path"`

	// Resolvers contains descriptions of urls to fetch.
	Resolvers []Resolver `yson:"resolvers"`
}

type Config struct {
	// Cluster is a name of a cluster (eg hume).
	Cluster string `yson:"cluster"`

	// ProfilePath fills empty paths in services.
	ProfilePath string `yson:"path"`

	// ProfileType fills empty types in services.
	ProfileType string `yson:"profile_type"`

	// Period fills empty periods in services.
	Period time.Duration `yson:"period"`

	// Probability fills empty probabilities in services.
	Probability float64 `yson:"probability"`

	// DCPrefixLen fills empty dcs in fetchers from podsets.
	DCPrefixLen int `yson:"dc_prefix_len"`

	// Services is a descriptions of what profiles to fetch.
	Services []Service `yson:"services"`
}

type Configs struct {
	// Configs contains description of what profiles to fetch per cluster.
	Configs []Config `yson:"configs"`

	// Ports is a map that is used to acquire information,
	// what ports to use for different services.
	Ports map[string]int `yson:"ports,omitempty"`

	// DCs keys are supported data centers, if value length is greater then 0,
	// resolver will be replaced with its copies for all elements in the values slices,
	// used in the 'YPCluster' field of resolver.
	DCs map[string][]string `yson:"dcs,omitempty"`
}

func (cs *Configs) FillInfo() error {
	for i := 0; i < len(cs.Configs); i++ {
		if cs.Ports != nil {
			cs.Configs[i].FillPorts(cs.Ports)
		}

		cs.Configs[i].FillInfo()

		err := cs.Configs[i].FillYPClusters(cs.DCs)
		if err != nil {
			return err
		}
	}

	return nil
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

func (c *Config) FillYPClusters(dcs map[string][]string) error {
	for i := 0; i < len(c.Services); i++ {
		if c.DCPrefixLen != 0 {
			for j := 0; j < len(c.Services[i].Resolvers); j++ {
				if c.Services[i].Resolvers[j].YPCluster == "" {
					dc := c.Services[i].Resolvers[j].YPPodSet[:c.DCPrefixLen]
					if _, ok := dcs[dc]; ok {
						c.Services[i].Resolvers[j].YPCluster = dc
					} else {
						return fmt.Errorf("prefix of %s - %s is not in dc list",
							c.Services[i].Resolvers[j].YPPodSet,
							dc,
						)
					}
				}
			}
		}

		if dcs != nil {
			filledResolvers := make([]Resolver, 0, len(c.Services[i].Resolvers))
			for j := 0; j < len(c.Services[i].Resolvers); j++ {
				dcslice, ok := dcs[c.Services[i].Resolvers[j].YPCluster]
				if !ok {
					return fmt.Errorf("%s is not in dc list",
						c.Services[i].Resolvers[j].YPCluster,
					)
				}

				if len(dcslice) <= 0 {
					filledResolvers = append(filledResolvers, c.Services[i].Resolvers[j])
				} else {
					for _, dc := range dcslice {
						c.Services[i].Resolvers[j].YPCluster = dc
						filledResolvers = append(filledResolvers, c.Services[i].Resolvers[j])
					}
				}

			}
			c.Services[i].Resolvers = filledResolvers
		}
	}

	return nil
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
