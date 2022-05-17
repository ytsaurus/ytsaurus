package fetcher

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/pprof/profile"

	"a.yandex-team.ru/infra/yp_service_discovery/golang/resolver"
	"a.yandex-team.ru/infra/yp_service_discovery/golang/resolver/httpresolver"
	"a.yandex-team.ru/library/go/core/log"
	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

type (
	Fetcher struct {
		config      Config
		yc          yt.Client
		tableYTPath ypath.Path

		storage *storage.TableStorage

		services []ServiceFetcher

		l *logzap.Logger
	}

	ServiceFetcher struct {
		service Service
		f       *Fetcher

		resolvers []ResolverFetcher
	}

	ResolverFetcher struct {
		resolver Resolver
		r        *httpresolver.Resolver
		sf       *ServiceFetcher
	}
)

func NewFetcher(yc yt.Client, config Config, l *logzap.Logger) *Fetcher {
	f := new(Fetcher)
	f.yc = yc
	f.config = config
	f.l = l
	f.tableYTPath = ypath.Path(config.TablePath)
	f.storage = storage.NewTableStorage(yc, f.tableYTPath, l)

	f.services = make([]ServiceFetcher, len(config.Services))
	for id, service := range config.Services {
		f.services[id] = *NewServiceFetcher(service, f)
	}

	return f
}

func NewServiceFetcher(service Service, f *Fetcher) *ServiceFetcher {
	sf := new(ServiceFetcher)
	sf.service = service
	sf.f = f

	sf.resolvers = make([]ResolverFetcher, len(service.Resolvers))
	for id, resolver := range service.Resolvers {
		sf.resolvers[id] = *NewResolverFetcher(resolver, sf)
	}

	return sf
}

func NewResolverFetcher(resolver Resolver, sf *ServiceFetcher) *ResolverFetcher {
	rf := new(ResolverFetcher)
	rf.resolver = resolver
	rf.sf = sf

	var err error
	rf.r, err = httpresolver.New()
	if err != nil {
		rf.sf.f.l.Fatal("error while creating httpresolver", log.Error(err))
	}

	return rf
}

func (f *Fetcher) RunFetcherContinious() error {
	err := ytprof.MigrateTables(f.yc, f.tableYTPath)

	if err != nil {
		f.l.Fatal("migraton failed", log.Error(err), log.String("table_path", f.tableYTPath.String()))
	}

	f.l.Debug("migraton succeded", log.String("table_path", f.tableYTPath.String()))

	rand.Seed(time.Now().UnixMicro())

	for _, service := range f.services {
		go func(serviceFetcher ServiceFetcher) {
			serviceFetcher.runServiceFetcherContinious()
		}(service)
	}

	select {}
}

func (sf *ServiceFetcher) runServiceFetcherContinious() {
	for {
		go sf.fetchService()

		time.Sleep(sf.service.Period)
	}
}

func (sf *ServiceFetcher) fetchService() {
	sz := len(sf.resolvers)

	results := make([][]*profile.Profile, sz)
	resultslice := make([]*profile.Profile, 0)
	hosts := make([][]string, sz)
	hostslice := make([]string, 0)
	errs := make([][]error, sz)

	sf.f.l.Debug("all corutines getting started", log.String("profile_service", sf.service.ProfilePath))

	var wg sync.WaitGroup
	wg.Add(sz)
	for id, resolver := range sf.resolvers {
		go func(id int, resolver ResolverFetcher) {
			defer wg.Done()
			results[id], hosts[id], errs[id] = resolver.fetchResolver()
		}(id, resolver)
	}

	sf.f.l.Debug("all corutines started", log.String("profile_service", sf.service.ProfilePath))

	wg.Wait()

	sf.f.l.Debug("all corutines finished", log.String("profile_service", sf.service.ProfilePath))

	for i := 0; i < len(results); i++ {
		for j := 0; j < len(results[i]); j++ {
			if len(errs) <= i || len(errs[i]) <= j {
				sf.f.l.Error("error while running fetch service (errs and results size don't match)")
				continue
			}

			if errs[i][j] != nil {
				sf.f.l.Error("error while running fetch service", log.String("cluster", sf.f.config.Cluster), log.Error(errs[i][j]))
				continue
			}

			resultslice = append(resultslice, results[i][j])
			hostslice = append(hostslice, hosts[i][j])
		}
	}

	sf.f.l.Debug("getting ready to push data", log.Int("data_size", len(resultslice)))
	err := sf.f.storage.PushData(context.Background(), resultslice, hostslice, sf.service.ProfileType, sf.f.config.Cluster, sf.service.ServiceType)
	if err != nil {
		sf.f.l.Error("error while storing profiles", log.String("cluster", sf.f.config.Cluster), log.Error(err))
	}
}

func (rf *ResolverFetcher) resolveFQDNs() ([]string, error) {
	if len(rf.resolver.YPEndpoint) > 0 {
		var err error
		ctx := context.Background()
		respEndpoint, err := rf.r.ResolveEndpoints(ctx, rf.resolver.YPCluster, rf.resolver.YPEndpoint)
		if err != nil {
			rf.sf.f.l.Error("error while resolving endpoint", log.Error(err))
			return nil, err
		}

		if respEndpoint.ResolveStatus != resolver.StatusEndpointOK {
			rf.sf.f.l.Error("not ok response status of endpoint",
				log.Int("status", respEndpoint.ResolveStatus),
				log.Int("statusOK", int(resolver.StatusEndpointOK)),
				log.Int("statusEmpty", int(resolver.StatusEndpointEmpty)),
				log.Int("statusNotExists", int(resolver.StatusEndpointNotExists)))
			return nil, err
		}

		rf.sf.f.l.Debug("url resolving finished",
			log.String("cluster", rf.resolver.YPCluster),
			log.String("endpoint", rf.resolver.YPEndpoint),
			log.Int("resolve_status", respEndpoint.ResolveStatus))

		sliceFQDN := make([]string, len(respEndpoint.EndpointSet.Endpoints))
		for i := 0; i < len(sliceFQDN); i++ {
			sliceFQDN[i] = respEndpoint.EndpointSet.Endpoints[i].FQDN
		}

		return sliceFQDN, nil
	} else if len(rf.resolver.YPPodSet) > 0 {
		var err error
		ctx := context.Background()
		respPodSet, err := rf.r.ResolvePods(ctx, rf.resolver.YPCluster, rf.resolver.YPPodSet)
		if err != nil {
			rf.sf.f.l.Error("error while resolving podset", log.Error(err))
			return nil, err
		}

		if respPodSet.ResolveStatus != resolver.StatusPodOK {
			rf.sf.f.l.Error("not ok response status of podset",
				log.Int("status", respPodSet.ResolveStatus),
				log.Int("statusOK", int(resolver.StatusPodOK)),
				log.Int("statusEmpty", int(resolver.StatusPodEmpty)),
				log.Int("statusNotExists", int(resolver.StatusPodNotExists)))
			return nil, err
		}

		rf.sf.f.l.Debug("url resolving finished",
			log.String("cluster", rf.resolver.YPCluster),
			log.String("podset", rf.resolver.YPPodSet),
			log.Int("resolve_status", respPodSet.ResolveStatus))

		sliceFQDN := make([]string, len(respPodSet.PodSet.Pods))
		for i := 0; i < len(sliceFQDN); i++ {
			sliceFQDN[i] = respPodSet.PodSet.Pods[i].DNS.PersistentFQDN
		}

		return sliceFQDN, nil
	}

	return rf.resolver.Urls, nil
}

func (rf *ResolverFetcher) fetchResolver() ([]*profile.Profile, []string, []error) {
	var usedIDs []int
	sliceFQDNs, err := rf.resolveFQDNs()
	if err != nil {
		return nil, nil, nil
	}

	for i := 0; i < len(sliceFQDNs); i++ {
		result := rand.Float64()
		if result < rf.sf.service.Probability {
			usedIDs = append(usedIDs, i)
		}
	}

	errs := make([]error, len(usedIDs))
	results := make([]*profile.Profile, len(usedIDs))
	hosts := make([]string, len(usedIDs))

	var wg sync.WaitGroup
	wg.Add(len(usedIDs))
	for i := 0; i < len(usedIDs); i++ {
		go func(id int) {
			defer wg.Done()
			hosts[id] = sliceFQDNs[usedIDs[id]]
			results[id], errs[id] = rf.fetchURL(fmt.Sprintf("http://%v", hosts[id]))
		}(i)
	}

	wg.Wait()

	rf.sf.f.l.Debug("service resolver finished", log.String("service", rf.sf.service.ServiceType), log.Int("profiles_fetched", len(results)))
	return results, hosts, errs
}

func (rf *ResolverFetcher) fetchURL(url string) (*profile.Profile, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	requestURL := fmt.Sprintf("%v:%d/%v", url, rf.resolver.Port, rf.sf.service.ProfilePath)
	rf.sf.f.l.Debug("sending request", log.String("request_url", requestURL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("not ok response stasus (%v)", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return profile.ParseData(body)
}
