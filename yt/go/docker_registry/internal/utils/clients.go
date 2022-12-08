package utils

import (
	"context"
	"fmt"
	"os"
	"strings"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"

	dcontext "github.com/distribution/distribution/v3/context"
)

const (
	ytClustersEnvName = "DOCKER_REGISTRY_YT_CLUSTERS"
	ytProxyEnvName    = "YT_PROXY"
)

type YTClients struct {
	clients           map[string]yt.Client
	defaultClientName string
	homeDirectory     string
}

func (c *YTClients) Init() error {
	c.clients = make(map[string]yt.Client)
	c.defaultClientName = os.Getenv(ytProxyEnvName)

	logger, stop := GetLogger()
	defer stop()

	if c.defaultClientName != "" {
		config := &yt.Config{Proxy: c.defaultClientName, Logger: logger, ReadTokenFromFile: true}
		yc, err := ythttp.NewClient(config)
		if err != nil {
			return err
		}
		c.clients[c.defaultClientName] = yc
	}

	ytClusters := strings.Split(os.Getenv(ytClustersEnvName), " ")
	for _, clusterName := range ytClusters {
		config := &yt.Config{Proxy: clusterName, Logger: logger, ReadTokenFromFile: true}
		yc, err := ythttp.NewClient(config)
		if err != nil {
			return err
		}
		c.clients[clusterName] = yc
	}

	if len(c.clients) == 0 {
		return fmt.Errorf("no yt clusters provided")
	}
	return nil
}

func (c *YTClients) GetClientFromContext(ctx context.Context) (yt.Client, error) {
	req, _ := dcontext.GetRequest(ctx)
	if req == nil {
		if c.defaultClientName != "" {
			return c.clients[c.defaultClientName], nil
		} else {
			return nil, fmt.Errorf("no default yt cluster provided")
		}
	}

	// Expected Host name registry.CLUSTER_NAME.yt.yandex(.net|-team.ru):443
	// or
	// hardcoded localhost:5000 -> YT_PROXY
	var clusterName string
	if req.Host == "localhost:5000" {
		clusterName = c.defaultClientName
	} else {
		parts := strings.Split(req.Host, ".")
		if len(parts) != 5 {
			return nil, fmt.Errorf("bad registry fqdn")
		}
		clusterName = strings.Split(req.Host, ".")[1]
	}
	v, ok := c.clients[clusterName]
	if !ok {
		return nil, fmt.Errorf("can't find %q cluster in %q env variable", clusterName, ytClustersEnvName)
	}
	return v, nil
}

func (c *YTClients) GetClients() []yt.Client {
	clients := make([]yt.Client, 0, len(c.clients))
	for _, v := range c.clients {
		clients = append(clients, v)
	}
	return clients
}
