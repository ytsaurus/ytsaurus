package tech.ytsaurus.client.discovery;

import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/**
 * Discoverer with unmodifiable list of discovery servers addresses.
 */
public class StaticDiscoverer implements Discoverer {
    private static final String ENV_NAME = "YT_DISCOVERY_ADDRESSES";

    private final List<String> addresses;

    /**
     * Create discoverer from list of servers addresses.
     */
    public StaticDiscoverer(List<String> addresses) {
        this.addresses = List.copyOf(addresses);
    }

    /**
     * Create discoverer from list node with servers addresses.
     */
    public StaticDiscoverer(YTreeNode node) {
        this(node.asList().stream().map(YTreeNode::stringValue).collect(Collectors.toList()));
    }

    /**
     * Create discoverer from servers addresses specified in environment variable.
     */
    public static StaticDiscoverer loadFromEnvironment() {
        String rawList = System.getenv(ENV_NAME);
        if (rawList == null || rawList.isEmpty()) {
            throw new IllegalStateException(String.format("Environment variable %s is not found", ENV_NAME));
        }
        return new StaticDiscoverer(YTreeTextSerializer.deserialize(rawList));
    }

    @Override
    public List<String> listDiscoveryServers() {
        return addresses;
    }

    @Override
    public void stop() {
    }
}
