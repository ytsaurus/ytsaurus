package fetcher

import (
	"fmt"
	"strings"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
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

	// Services is a descriptions of what profiles to fetch.
	Services []Service `yson:"services"`
}

type Configs struct {
	// Configs contains description of what profiles to fetch per cluster.
	Configs []Config `yson:"configs"`

	// Ports is a map that is used to acquire information,
	// what ports to use for different services.
	Ports map[string]int `yson:"ports,omitempty"`

	// YPClusters replaces empty 'YPCluster'-s in resolver.
	YPClusters []string `yson:"yp_clusters,omitempty"`
}

func (cs *Configs) FillInfo(l *zap.Logger) {
	for i := 0; i < len(cs.Configs); i++ {
		cs.Configs[i].FillInfo(l)

		if cs.Ports != nil {
			cs.Configs[i].FillPorts(cs.Ports)
		}

		cs.Configs[i].FillYPClusters(cs.YPClusters)
	}
}

func (c *Config) FillInfo(l *zap.Logger) {
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

		if c.Services[i].ServiceType == "" && len(c.Services[i].Resolvers) > 0 {
			if c.Services[i].Resolvers[0].YPPodSet != "" {
				var err error
				c.Services[i].ServiceType, err = serviceNameByPodset(c.Services[i].Resolvers[0].YPPodSet)
				if err != nil {
					l.Error("podset not recognized",
						log.Error(err),
						log.String("podset", c.Services[i].Resolvers[0].YPPodSet),
					)
				}
			}
		}
	}
}

func (c *Config) FillYPClusters(ypClusters []string) {
	for i := 0; i < len(c.Services); i++ {
		filledResolvers := make([]Resolver, 0)
		for j := 0; j < len(c.Services[i].Resolvers); j++ {
			for _, ypCluster := range ypClusters {
				c.Services[i].Resolvers[j].YPCluster = ypCluster
				filledResolvers = append(filledResolvers, c.Services[i].Resolvers[j])
			}
		}
		c.Services[i].Resolvers = filledResolvers
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

func serviceNameByPodset(podset string) (string, error) {
	if strings.Contains(podset, "master-cache") {
		return "master-caches", nil
	}
	if strings.Contains(podset, "master") {
		return "master", nil
	}
	if strings.Contains(podset, "scheduler") {
		return "scheduler", nil
	}
	if strings.Contains(podset, "clock") {
		return "clock", nil
	}
	if strings.Contains(podset, "controller-agent") {
		return "controller-agent", nil
	}
	if strings.Contains(podset, "rpc-prox") {
		return "rpc-proxy", nil
	}
	if strings.Contains(podset, "prox") {
		return "http-proxy", nil
	}
	if strings.Contains(podset, "exe-tab-node") {
		return "exe-tab-node", nil
	}
	if strings.Contains(podset, "exe-node") {
		return "exe-node", nil
	}
	if strings.Contains(podset, "tab-node") || strings.Contains(podset, "tablet-node") {
		return "tab-node", nil
	}
	if strings.Contains(podset, "dat-node") {
		return "dat-node", nil
	}
	if strings.Contains(podset, "chaos-node") {
		return "chaos-node", nil
	}
	if strings.Contains(podset, "node") {
		return "node", nil
	}
	if strings.Contains(podset, "discovery") {
		return "discovery", nil
	}
	if strings.Contains(podset, "timestamp-provider") {
		return "timestamp-provider", nil
	}

	return "none", fmt.Errorf("service not found")
}
