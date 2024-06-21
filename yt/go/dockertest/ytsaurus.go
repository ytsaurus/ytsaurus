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
)

const ytsaurusPath = "/home/yt"

type YTsaurusContainter struct {
	testcontainers.Container

	Proxy   string
	Cluster string
}

// RunContainer creates an instance of the YTsaurus container type.
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*YTsaurusContainter, error) {
	httpProxyPort, err := getAvailablePort()
	if err != nil {
		return nil, err
	}

	cluster := guid.New().String()

	req := testcontainers.ContainerRequest{
		Image: "ghcr.io/ytsaurus/local:dev",
		Cmd: []string{
			"--fqdn", "localhost",
			"--proxy-config", fmt.Sprintf("{address_resolver={enable_ipv4=%%true;enable_ipv6=%%false;};coordinator={public_fqdn=\"localhost:%d\"}}", httpProxyPort),
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

	return &YTsaurusContainter{Container: container, Cluster: cluster, Proxy: fmt.Sprintf("localhost:%d", httpProxyPort)}, nil
}

func WithDynamicTables() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "--wait-tablet-cell-initialization")

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
