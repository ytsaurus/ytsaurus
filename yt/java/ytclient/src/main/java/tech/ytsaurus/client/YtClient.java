package tech.ytsaurus.client;

import java.util.Arrays;
import java.util.List;

import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcCredentials;
import tech.ytsaurus.client.rpc.RpcOptions;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.library.svnversion.VcsVersion;
import ru.yandex.yt.ytclient.proxy.YandexSerializationResolver;

public class YtClient extends tech.ytsaurus.client.YTsaurusClient {
    private static final String USER_AGENT = calcUserAgent();

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
                new YTsaurusClient.Builder()
                        .setSharedBusConnector(connector)
                        .setClusters(clusters)
                        .setPreferredClusterName(localDataCenterName)
                        .setProxyRole(proxyRole)
                        .setRpcCredentials(credentials)
                        .setRpcCompression(compression)
                        .setYtClientConfiguration(YtClientConfiguration.builder()
                                .setRpcOptions(options)
                                .setVersion(USER_AGENT)
                                .build())
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
    public static class Builder extends YTsaurusClient.ClientBuilder<YtClient, Builder> {
        @Override
        protected Builder self() {
            return this;
        }



        /**
         * Finally create a client.
         */
        @Override
        public YtClient build() {
            return new YtClient(new YTsaurusClient.BuilderWithDefaults(this));
        }
    }

    private static String calcUserAgent() {
        VcsVersion version = new VcsVersion(YtClientConfiguration.class);
        return "yt/java/ytclient@" + version.getProgramSvnRevision();
    }
}
