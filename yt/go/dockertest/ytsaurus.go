package dockertest

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
)

const ytsaurusPath = "/home/yt"

type YTsaurusContainer struct {
	testcontainers.Container

	Proxy   string
	Cluster string
}

// RunContainer creates an instance of the YTsaurus container type.
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*YTsaurusContainer, error) {
	httpProxyPort, err := getAvailablePort()
	if err != nil {
		return nil, err
	}

	cluster := guid.New().String()

	proxyConfig := map[string]any{
		"address_resolver": map[string]any{
			"enable_ipv4": true,
			"enable_ipv6": false,
		},
		"coordinator": map[string]any{
			"public_fqdn": fmt.Sprintf("localhost:%d", httpProxyPort),
		},
	}
	for _, opt := range opts {
		if patch, ok := opt.(proxyConfigPatch); ok {
			patchMap(proxyConfig, patch)
		}
	}
	proxyConfigYSON, err := yson.Marshal(proxyConfig)
	if err != nil {
		return nil, err
	}

	req := testcontainers.ContainerRequest{
		Image: "ghcr.io/ytsaurus/local:dev",
		Cmd: []string{
			"--fqdn", "localhost",
			"--proxy-config", string(proxyConfigYSON),
			"--path", ytsaurusPath,
			"--proxy-port", strconv.Itoa(httpProxyPort),
			"--id", cluster,
		},
		ExposedPorts: []string{fmt.Sprintf("%d:%d", httpProxyPort, httpProxyPort)},
		WaitingFor:   wait.ForLog("Local YT started").WithStartupTimeout(10 * time.Minute),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	idInd := slices.Index(req.Cmd, "--id")
	if idInd != -1 {
		cluster = req.Cmd[idInd+1]
	}

	return &YTsaurusContainer{Container: container, Cluster: cluster, Proxy: fmt.Sprintf("localhost:%d", httpProxyPort)}, nil
}

func WithDynamicTables() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "--wait-tablet-cell-initialization")

		return nil
	}
}

func WithSecondaryMasterCells(count int) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "--secondary-master-cell-count", strconv.Itoa(count))

		return nil
	}
}

func WithClusterName(name string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		idInd := slices.Index(req.Cmd, "--id")
		if idInd != -1 {
			req.Cmd[idInd+1] = name

			return nil
		}

		req.Cmd = append(req.Cmd, "--id", name)

		return nil
	}
}

func WithRPCProxies(rpcProxiesCount int) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		rpcProxyPort, err := getAvailablePort()
		if err != nil {
			return err
		}

		req.Cmd = append(req.Cmd, "--rpc-proxy-count", strconv.Itoa(rpcProxiesCount))
		req.Cmd = append(req.Cmd, "--rpc-proxy-port", strconv.Itoa(rpcProxyPort))

		req.ExposedPorts = append(req.ExposedPorts, fmt.Sprintf("%d:%d", rpcProxyPort, rpcProxyPort))

		return nil
	}
}

func WithHTTPProxiesCount(proxiesCount int) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "--http-proxy-count", strconv.Itoa(proxiesCount))

		return nil
	}
}

func WithNodesCount(nodesCount int) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "--nodes-count", strconv.Itoa(nodesCount))

		return nil
	}
}

func WithDiscoveryServers(serversCount int) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		serverPort, err := getAvailablePort()
		if err != nil {
			return err
		}

		req.Cmd = append(req.Cmd, "--discovery-server-count", strconv.Itoa(serversCount))
		req.Cmd = append(req.Cmd, "--discovery-server-port", strconv.Itoa(serverPort))

		req.ExposedPorts = append(req.ExposedPorts, fmt.Sprintf("%d:%d", serverPort, serverPort))

		return nil
	}
}

// WithVolumeMount mounts the specified volume to ytsaurusPath inside the container.
func WithVolumeMount(volume string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Mounts = append(req.Mounts, testcontainers.VolumeMount(volume, ytsaurusPath))

		return nil
	}
}

func WithOperationsArchive() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "--init-operations-archive")

		return nil
	}
}

type proxyConfigPatch map[string]any

func (proxyConfigPatch) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithProxyConfigPatch deep-merges patch into the HTTP proxy --proxy-config.
func WithProxyConfigPatch(patch map[string]any) testcontainers.ContainerCustomizer {
	return proxyConfigPatch(patch)
}

func patchMap(dst, src map[string]any) {
	for k, v := range src {
		if sv, ok := v.(map[string]any); ok {
			if dv, ok := dst[k].(map[string]any); ok {
				patchMap(dv, sv)
				continue
			}
		}
		dst[k] = v
	}
}

func getAvailablePort() (int, error) {
	address, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:0", "0.0.0.0"))
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port, nil
}
