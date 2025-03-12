package tech.ytsaurus.testlib;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import tech.ytsaurus.lang.NonNullApi;

/**
 * Class helps to create YTsaurus clients to local YTsaurus instance.
 */
@NonNullApi
public class LocalYTsaurus {
    private static volatile Address address;
    private static volatile GenericContainer<?> container;

    private LocalYTsaurus() {
    }

    public static @Nullable GenericContainer<?> getContainer() {
        return container;
    }

    /**
     * Get full address of local YTsaurus instance from YT_PROXY environment variable.
     */
    public static String getAddress() {
        return getAddressInstance().host + ":" + getAddressInstance().port;
    }

    /**
     * Get host part of local YTsaurus instance address.
     */
    public static String getHost() {
        return container != null ? container.getHost() : getAddressInstance().host;
    }

    /**
     * Get port part of local YTsaurus instance address.
     */
    public static int getPort() {
        return container != null ? container.getMappedPort(80) : getAddressInstance().port;
    }

    public static synchronized void startContainer(Config config) {
        if (container != null) {
            container.stop();
        }

        GenericContainer<?> localYTsaurus = new FixedHostPortGenericContainer<>("ghcr.io/ytsaurus/local:dev")
                .withFixedExposedPort(10110, 80) // http
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
                commandParts.add("--rpc-proxy-port");
                commandParts.add(String.valueOf(config.rpcProxyPorts.get(i)));
                localYTsaurus.addExposedPort(config.rpcProxyPorts.get(i));
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
                commandParts.add("--discovery-server-port");
                commandParts.add(String.valueOf(config.discoveryServerPorts.get(i)));
                localYTsaurus.addExposedPort(config.discoveryServerPorts.get(i));
            }
        }
        if (config.secondaryMasterCellCount > 0) {
            commandParts.add("--secondary-master-cell-count");
            commandParts.add(String.valueOf(config.secondaryMasterCellCount));
        }

        localYTsaurus.withCommand(commandParts.toArray(new String[0]));
        localYTsaurus.waitingFor(Wait.forLogMessage(".*Local YT started.*", 1));
        localYTsaurus.start();

        container = localYTsaurus;
    }

    private static Address getAddressInstance() {
        Address tmpAddress = address;
        if (tmpAddress == null) {
            synchronized (LocalYTsaurus.class) {
                tmpAddress = address;
                if (tmpAddress == null) {
                    tmpAddress = new Address();
                    address = tmpAddress;
                }
            }
        }
        return tmpAddress;
    }

    public static class Config {
        private int rpcProxyCount;
        private List<Integer> rpcProxyPorts;
        private int queueAgentCount;
        private int discoveryServerCount;
        private List<Integer> discoveryServerPorts;
        private int secondaryMasterCellCount;
        private MountableFile rpcProxyConfigFile;
        private MountableFile proxyConfigFile;

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

    static class Address {
        private final String host;
        private final int port;

        private Address() {
            final String ytProxy = System.getenv("YT_PROXY");
            if (ytProxy == null) {
                throw new RuntimeException(
                        "Cannot get address of local YTsaurus instance: environment variable $YT_PROXY is not set."
                );
            }
            String[] parts = ytProxy.split(":");
            host = parts[0];
            if (parts.length == 1) {
                port = 80;
            } else if (parts.length == 2) {
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException error) {
                    throw new RuntimeException("Environment variable $YT_PROXY contains bad address: " + ytProxy,
                            error);
                }
            } else {
                throw new RuntimeException("Environment variable $YT_PROXY contains bad address: " + ytProxy);
            }

            // Check that we don't try to use production instance.
            if (host.contains(".yt.yandex.net")
                    || !host.contains(".") && !host.equals("localhost")) {
                String confirmation = System.getenv("YT_DANGEROUS_CONFIRMATION");
                if (confirmation == null || !confirmation.equals("hereby I confirm that I understand consequences of " +
                        "my actions")) {
                    // NB. It's dangerous to run tests on the production instance.
                    // (Especially if you are developer of YT).
                    // We cannot stop you if you really want, but you have to understand that YOU MIGHT BREAK
                    // EVERYTHING.
                    throw new RuntimeException(
                            "Looks like you are trying to use production YTsaurus instance in your tests: "
                                    + ytProxy + "\n" +
                                    "Please investigate source code and fill confirmation if you really want to do so."
                    );
                }
            }
        }
    }
}
