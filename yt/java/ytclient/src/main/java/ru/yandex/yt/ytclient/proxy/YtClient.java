package ru.yandex.yt.ytclient.proxy;

import java.util.Arrays;
import java.util.List;

import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcCredentials;
import tech.ytsaurus.client.rpc.RpcOptions;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.bus.BusConnector;

public class YtClient extends ru.yandex.yt.ytclient.proxy.YtClientOpensource {
    /**
     * @deprecated prefer to use {@link #builder()}
     */
    @Deprecated
    public YtClient(
            BusConnector connector,
            List<YtCluster> clusters,
            String localDataCenterName,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, clusters, localDataCenterName, null, credentials, options);
    }

    /**
     * @deprecated prefer to use {@link #builder()}
     */
    @Deprecated
    public YtClient(
            BusConnector connector,
            List<YtCluster> clusters,
            String localDataCenterName,
            String proxyRole,
            RpcCredentials credentials,
            RpcOptions options) {
        this(connector, clusters, localDataCenterName, proxyRole, credentials, new RpcCompression(), options);
    }

    /**
     * @deprecated prefer to use {@link #builder()}
     */
    @Deprecated
    public YtClient(
            BusConnector connector,
            List<YtCluster> clusters,
            String localDataCenterName,
            String proxyRole,
            RpcCredentials credentials,
            RpcCompression compression,
            RpcOptions options
    ) {
        super(new BuilderWithDefaults<>(
                new YtClientOpensource.Builder()
                        .setSharedBusConnector(connector)
                        .setClusters(clusters)
                        .setPreferredClusterName(localDataCenterName)
                        .setProxyRole(proxyRole)
                        .setRpcCredentials(credentials)
                        .setRpcCompression(compression)
                        .setRpcOptions(options)
                        .disableValidation()
                // old constructors did not validate their arguments
        ), YandexSerializationResolver.getInstance());
    }

    public YtClient(BusConnector connector, YtCluster cluster, RpcCredentials credentials, RpcOptions options) {
        this(connector, Arrays.asList(cluster), cluster.getName(), credentials, options);
    }

    public YtClient(BusConnector connector, String clusterName, RpcCredentials credentials, RpcOptions options) {
        this(connector, new YtCluster(clusterName), credentials, options);
    }

    public YtClient(BusConnector connector, String clusterName, RpcCredentials credentials) {
        this(connector, clusterName, credentials, new RpcOptions());
    }

    private YtClient(BuilderWithDefaults builder) {
        super(builder, YandexSerializationResolver.getInstance());
    }

    /**
     * Create builder for YtClient.
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends YtClientOpensource.ClientBuilder<YtClient, Builder> {
        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Finally create a client.
         */
        @Override
        public YtClient build() {
            return new YtClient(new YtClientOpensource.BuilderWithDefaults(this));
        }
    }
}
