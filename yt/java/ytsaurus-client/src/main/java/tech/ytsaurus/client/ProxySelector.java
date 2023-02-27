package tech.ytsaurus.client;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * ProxySelector subclasses are used to set priorities for available rpc proxies
 * <br>
 * Subclasses should implement {@link ProxySelector#doRank(List)} method,
 * which sorts that list of available proxies in a desired way
 * <br>
 * After sorting, top N proxies will be selected for client pool,
 * where N is equal to {@link RpcOptions#getChannelPoolSize()}
 */
@NonNullApi
@NonNullFields
public abstract class ProxySelector {

    protected final Random random = new Random();

    final void rank(List<HostPort> availableProxies) {
        Collections.shuffle(availableProxies);
        doRank(availableProxies);
    }

    abstract void doRank(List<HostPort> availableProxies);

    /**
     * A {@link ProxySelector} that selects proxies randomly
     */
    public static ProxySelector random() {
        return new RandomProxySelector();
    }

    /**
     * {@link ProxySelector} that avoids selecting proxies from a given DC
     */
    public static ProxySelector pessimizing(DC dataCenter) {
        return new PessimizingProxySelector(dataCenter);
    }

    /**
     * {@link ProxySelector} that prefers proxies from a given DC
     */
    public static ProxySelector preferring(DC dataCenter) {
        return new PreferringProxySelector(dataCenter);
    }

    /**
     * {@link ProxySelector} that prefers proxies in the same DC where ytclient is running on
     * If DC resolution fails (e.g. when running on local environment), no ranking of proxy list will occur
     */
    public static ProxySelector proximityBased() {
        DC currentDc = DC.getCurrentDc();

        if (currentDc == DC.UNKNOWN) {
            // if DC resolution fails, default to random proxy selector
            return new RandomProxySelector();
        }
        return new PreferringProxySelector(currentDc);
    }

    @NonNullApi
    @NonNullFields
    private static final class RandomProxySelector extends ProxySelector {

        @Override
        void doRank(List<HostPort> availableProxies) {
            // no-op
        }
    }

    @NonNullApi
    @NonNullFields
    private static final class PreferringProxySelector extends ProxySelector {

        private final DC preferredDc;
        private final Comparator<HostPort> proxyHostComparator;

        PreferringProxySelector(DC preferredDc) {
            if (preferredDc == DC.UNKNOWN) {
                throw new IllegalArgumentException("Enum value UNKNOWN is not allowed");
            }
            this.preferredDc = preferredDc;
            this.proxyHostComparator = createComparator();
        }

        @Override
        void doRank(List<HostPort> availableProxies) {
            availableProxies.sort(proxyHostComparator);
        }

        private Comparator<HostPort> createComparator() {
            // Proxies from preferred DC go before other proxies
            return Comparator.comparing(p -> p.getHost().startsWith(preferredDc.prefix()) ? 0 : 1);
        }
    }

    @NonNullApi
    @NonNullFields
    private static final class PessimizingProxySelector extends ProxySelector {

        private final DC pessimizedDc;
        private final Comparator<HostPort> proxyHostComparator;

        PessimizingProxySelector(DC pessimizedDc) {
            if (pessimizedDc == DC.UNKNOWN) {
                throw new IllegalArgumentException("Enum value UNKNOWN is not allowed");
            }
            this.pessimizedDc = pessimizedDc;
            this.proxyHostComparator = createComparator();
        }

        @Override
        void doRank(List<HostPort> availableProxies) {
            availableProxies.sort(proxyHostComparator);
        }

        private Comparator<HostPort> createComparator() {
            // Proxies from pessimized DC go after other proxies
            return Comparator.comparing(p -> p.getHost().startsWith(pessimizedDc.prefix()) ? 1 : 0);
        }
    }
}
