package tech.ytsaurus.client;

import java.util.Collections;
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

    @NonNullApi
    @NonNullFields
    static final class RandomProxySelector extends ProxySelector {

        @Override
        void doRank(List<HostPort> availableProxies) {
            // no-op
        }
    }

}
