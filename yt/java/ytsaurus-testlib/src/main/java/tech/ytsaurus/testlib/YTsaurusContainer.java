package tech.ytsaurus.testlib;

import java.util.ArrayList;
import java.util.List;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class YTsaurusContainer {
    public static synchronized GenericContainer<?> startContainer(Config config) {
        FixedHostPortGenericContainer<?> localYTsaurus =
                new FixedHostPortGenericContainer<>("ghcr.io/ytsaurus/local:dev")
                        .withFixedExposedPort(config.httpPort, 80) // http
                        .withNetwork(Network.newNetwork());

        List<String> commandParts = new ArrayList<>(List.of("--enable-debug-logging"));

        if (config.proxyConfigFile != null) {
            localYTsaurus.withCopyFileToContainer(config.proxyConfigFile, "/tmp/proxy_config.yson");
            commandParts.add("--proxy-config");
            commandParts.add("/tmp/proxy_config.yson");
        }
        if (config.rpcProxyConfigFile != null) {
            localYTsaurus.withCopyFileToContainer(config.rpcProxyConfigFile, "/tmp/rpc_proxy_config.yson");
            commandParts.add("--rpc-proxy-config");
            commandParts.add("/tmp/rpc_proxy_config.yson");
        }

        if (config.rpcProxyCount > 0) {
            commandParts.add("--rpc-proxy-count");
            commandParts.add(String.valueOf(config.rpcProxyCount));
            for (int i = 0; i < config.rpcProxyCount; i++) {
                int rpcProxyPort = config.rpcProxyPorts.get(i);
                commandParts.add("--rpc-proxy-port");
                commandParts.add(String.valueOf(rpcProxyPort));
                localYTsaurus.withFixedExposedPort(rpcProxyPort, rpcProxyPort);
            }
        }

        if (config.queueAgentCount > 0) {
            commandParts.add("--queue-agent-count");
            commandParts.add(String.valueOf(config.queueAgentCount));
        }
        if (config.discoveryServerCount > 0) {
            commandParts.add("--discovery-server-count");
            commandParts.add(String.valueOf(config.discoveryServerCount));
            for (int i = 0; i < config.discoveryServerCount; i++) {
                int discoveryServerPort = config.discoveryServerPorts.get(i);
                commandParts.add("--discovery-server-port");
                commandParts.add(String.valueOf(discoveryServerPort));
                localYTsaurus.withFixedExposedPort(discoveryServerPort, discoveryServerPort);
            }
        }
        if (config.secondaryMasterCellCount > 0) {
            commandParts.add("--secondary-master-cell-count");
            commandParts.add(String.valueOf(config.secondaryMasterCellCount));
        }

        localYTsaurus.withCommand(commandParts.toArray(new String[0]));
        localYTsaurus.waitingFor(Wait.forLogMessage(".*Local YT started.*", 1));
        localYTsaurus.start();

        return localYTsaurus;
    }

    public static class Config {
        private int httpPort;
        private int rpcProxyCount;
        private List<Integer> rpcProxyPorts;
        private int queueAgentCount;
        private int discoveryServerCount;
        private List<Integer> discoveryServerPorts;
        private int secondaryMasterCellCount;
        private MountableFile rpcProxyConfigFile;
        private MountableFile proxyConfigFile;

        public Config setHttpPort(int httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Config setRpcProxyCount(int rpcProxyCount) {
            this.rpcProxyCount = rpcProxyCount;
            return this;
        }

        public Config setRpcProxyPorts(List<Integer> rpcProxyPorts) {
            this.rpcProxyPorts = rpcProxyPorts;
            return this;
        }

        public Config setRpcProxyConfigFile(MountableFile configFile) {
            this.rpcProxyConfigFile = configFile;
            return this;
        }

        public Config setQueueAgentCount(int queueAgentCount) {
            this.queueAgentCount = queueAgentCount;
            return this;
        }

        public Config setDiscoveryServerCount(int discoveryServerCount) {
            this.discoveryServerCount = discoveryServerCount;
            return this;
        }

        public Config setDiscoveryServerPorts(List<Integer> discoveryServerPorts) {
            this.discoveryServerPorts = discoveryServerPorts;
            return this;
        }

        public Config setSecondaryMasterCellCount(int secondaryMasterCellCount) {
            this.secondaryMasterCellCount = secondaryMasterCellCount;
            return this;
        }

        public Config setProxyConfigFile(MountableFile file) {
            this.proxyConfigFile = file;
            return this;
        }
    }
}
