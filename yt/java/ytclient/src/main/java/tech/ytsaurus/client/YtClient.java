package tech.ytsaurus.client;

import java.util.List;

import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

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
            YTsaurusClientAuth auth,
            RpcOptions options) {
        this(connector, clusters, localDataCenterName, null, auth, options);
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
            YTsaurusClientAuth auth,
            RpcOptions options) {
        this(connector, clusters, localDataCenterName, proxyRole, auth, new RpcCompression(), options);
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
            YTsaurusClientAuth auth,
            RpcCompression compression,
            RpcOptions options
    ) {
        super(new BuilderWithDefaults<>(
                new YTsaurusClient.Builder()
                        .setSharedBusConnector(connector)
                        .setClusters(clusters)
                        .setPreferredClusterName(localDataCenterName)
                        .setProxyRole(proxyRole)
                        .setAuth(auth)
                        .setRpcCompression(compression)
                        .setYtClientConfiguration(YtClientConfiguration.builder()
                                .setRpcOptions(options)
                                .setVersion(USER_AGENT)
                                .build())
                        .disableValidation()
                // old constructors did not validate their arguments
        ), YandexSerializationResolver.getInstance());
    }

    public YtClient(BusConnector connector, YtCluster cluster, YTsaurusClientAuth auth, RpcOptions options) {
        this(connector, List.of(cluster), cluster.getName(), auth, options);
    }

    public YtClient(BusConnector connector, String clusterName, YTsaurusClientAuth auth, RpcOptions options) {
        this(connector, new YtCluster(clusterName), auth, options);
    }

    public YtClient(BusConnector connector, String clusterName, YTsaurusClientAuth auth) {
        this(connector, clusterName, auth, new RpcOptions());
    }

    private YtClient(BuilderWithDefaults builder) {
        super(builder, YandexSerializationResolver.getInstance());
    }

    /**
     * Create builder for YtClient.
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
