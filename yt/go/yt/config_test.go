package yt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterURL(t *testing.T) {
	type config struct {
		proxy         string
		defaultSuffix string
		useTLS        bool
		useTVM        bool
	}

	tvmHTTPPort := fmt.Sprint(TVMOnlyHTTPProxyPort)
	tvmHTTPSPort := fmt.Sprint(TVMOnlyHTTPSProxyPort)

	for _, test := range []struct {
		name        string
		config      config
		expectedURL string
	}{
		{
			"localhost",
			config{proxy: "localhost"},
			"http://localhost",
		},
		{
			"localhost w port",
			config{proxy: "localhost:23924"},
			"http://localhost:23924",
		},
		{
			"short name w port",
			config{proxy: "hostname:123"},
			"http://hostname:123",
		},
		{
			"short_host_w_scheme",
			config{proxy: "https://cluster1"},
			"https://cluster1",
		},
		{
			"cluster name",
			config{proxy: "cluster1"},
			"http://cluster1.yt.yandex.net",
		},
		{
			"proxy_fqdn",
			config{proxy: "sas4-5340-proxy-cluster1.man-pre.yp-c.yandex.net:80"},
			"http://sas4-5340-proxy-cluster1.man-pre.yp-c.yandex.net:80",
		},
		{
			"tvm_only",
			config{proxy: "cluster1", useTVM: true},
			"http://tvm.cluster1.yt.yandex.net:" + tvmHTTPPort,
		},
		{
			"tvm_only_https",
			config{proxy: "https://cluster1", useTVM: true},
			"https://tvm.cluster1:" + tvmHTTPSPort,
		},
		{
			"tvm_only_http",
			config{proxy: "http://cluster1", useTVM: true},
			"http://tvm.cluster1:" + tvmHTTPPort,
		},
		{
			"default_suffix",
			config{proxy: "cluster1", defaultSuffix: ".imaginary.yt.yandex.net"},
			"http://cluster1.imaginary.yt.yandex.net",
		},
		{
			"cluster_name config https",
			config{proxy: "cluster1", useTLS: true},
			"https://cluster1.yt.yandex.net",
		},
		{
			"localhost",
			config{proxy: "localhost", useTLS: true},
			"https://localhost",
		},
		{
			"localhost override",
			config{proxy: "http://localhost", useTLS: true},
			"http://localhost",
		},
		{
			"cluster_name url priority over config 1",
			config{proxy: "http://cluster1.yt.domain.net", useTLS: true},
			"http://cluster1.yt.domain.net",
		},
		{
			"cluster_name url priority over config 2",
			config{proxy: "https://cluster1.yt.domain.net", useTLS: false},
			"https://cluster1.yt.domain.net",
		},
		{
			"proxy_fqdn config https",
			config{proxy: "sas4-5340-proxy-cluster1.man-pre.yp-c.domain.net:80", useTLS: true},
			"https://sas4-5340-proxy-cluster1.man-pre.yp-c.domain.net:80",
		},
		{
			"tvm_only config https",
			config{proxy: "cluster1", useTVM: true, useTLS: true},
			"https://tvm.cluster1.yt.yandex.net:" + tvmHTTPSPort,
		},
		{
			"default_suffix config https",
			config{proxy: "cluster1", defaultSuffix: ".imaginary.yt.cluster.net", useTLS: true},
			"https://cluster1.imaginary.yt.cluster.net",
		},
		{
			"ipv4",
			config{proxy: "127.0.0.1"},
			"http://127.0.0.1",
		},
		{
			"ipv4 with port",
			config{proxy: "127.0.0.1:23924"},
			"http://127.0.0.1:23924",
		},
		{
			"ipv4 with scheme and port",
			config{proxy: "https://127.0.0.1:23924"},
			"https://127.0.0.1:23924",
		},
		{
			"ipv6",
			config{proxy: "[::1]"},
			"http://[::1]",
		},
		{
			"ipv4-mapped",
			config{proxy: "[::ffff:127.0.0.1]"},
			"http://[::ffff:127.0.0.1]",
		},
		{
			"ipv6 with port",
			config{proxy: "[::1]:23924"},
			"http://[::1]:23924",
		},
		{
			"ipv6 with scheme and port",
			config{proxy: "https://[::1]:23924"},
			"https://[::1]:23924",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.config.defaultSuffix != "" {
				t.Skip("default_suffix is not supported yet")
			}

			conf := Config{
				Proxy:              test.config.proxy,
				UseTLS:             test.config.useTLS,
				UseTVMOnlyEndpoint: test.config.useTVM,
			}

			clusterURL, err := conf.GetClusterURL()
			require.NoError(t, err)

			url := clusterURL.Scheme + "://" + clusterURL.Address
			require.Equal(t, test.expectedURL, url)
		})
	}
}
