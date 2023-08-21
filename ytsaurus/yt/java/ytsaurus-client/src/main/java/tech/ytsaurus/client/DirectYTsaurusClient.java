package tech.ytsaurus.client;

import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import javax.annotation.Nullable;

import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.DefaultRpcBusClient;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 *  YT client with fixed specified RPC proxy address.
 *  This client doesn't support proxy discovering,
 *  so if you need connecting to shared cluster then using {@link YTsaurusClient} is preferred.
 *  <p>
 *      Client supports a few types of address.
 *      1) InetSocketAddress - address of some remote RPC proxy
 *      2) DomainSocketAddress - address of socket file connected with RPC proxy
 */
@NonNullApi
@NonNullFields
public class DirectYTsaurusClient extends CompoundClientImpl {
    private final BusConnector busConnector;
    private final boolean isBusConnectorOwner;

    DirectYTsaurusClient(Builder builder) {
        super(
                new DefaultRpcBusClient(builder.busConnector, builder.address, builder.address.toString())
                        .withAuthentication(builder.auth),
                builder.busConnector.executorService(), builder.config, builder.heavyExecutor,
                builder.serializationResolver
        );

        this.busConnector = builder.busConnector;
        this.isBusConnectorOwner = builder.isBusConnectorOwner;
    }

    public static Builder builder() {
        return new DirectYTsaurusClient.Builder();
    }

    @Override
    public void close() {
        if (isBusConnectorOwner) {
            busConnector.close();
        }
        super.close();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        @Nullable
        BusConnector busConnector;
        boolean isBusConnectorOwner = true;
        @Nullable
        SocketAddress address;
        @Nullable
        YTsaurusClientAuth auth;
        @Nullable
        YTsaurusClientConfig config;
        @Nullable
        Executor heavyExecutor;
        @Nullable
        SerializationResolver serializationResolver;

        Builder() {
        }

        /**
         * Set BusConnector for DirectYTsaurusClient.
         *
         * <p>
         * Connector will be owned by DirectYTsaurusClient.
         * DirectYTsaurusClient will close it when {@link DirectYTsaurusClient#close()} is called.
         *
         * <p>
         * If bus is never set default bus will be created
         * (default bus will be owned by DirectYTsaurusClient, so you don't need to worry about closing it).
         */
        public Builder setOwnBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = true;
            return self();
        }

        /**
         * Set BusConnector for DirectYTsaurusClient.
         *
         * <p>
         * Connector will not be owned by DirectYTsaurusClient. It's user responsibility to close the connector.
         *
         * @see #setOwnBusConnector
         */
        public Builder setSharedBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = false;
            return self();
        }

        /**
         * Set RPC proxy address to use.
         *
         * @param address address of RPC proxy
         */
        public Builder setAddress(SocketAddress address) {
            this.address = address;
            return self();
        }

        /**
         * Set heavy executor for DirectYTsaurusClient. This is used for deserialization of lookup/select response.
         * By default, ForkJoinPool.commonPool().
         * @return self
         */
        public Builder setHeavyExecutor(Executor heavyExecutor) {
            this.heavyExecutor = heavyExecutor;
            return self();
        }

        /**
         * Set authentication information.
         *
         * <p>
         * When no {@code YTsaurusClientAuth} is set, username and token are loaded from environment.
         *
         * @see YTsaurusClientAuth#loadUserAndTokenFromEnvironment()
         */
        public Builder setAuth(YTsaurusClientAuth auth) {
            this.auth = auth;
            return self();
        }

        /**
         * Set settings of DirectYTsaurusClient.
         */
        public Builder setConfig(YTsaurusClientConfig config) {
            this.config = config;
            return self();
        }

        protected Builder self() {
            return this;
        }

        public DirectYTsaurusClient build() {
            if (busConnector == null) {
                busConnector = new DefaultBusConnector();
            }
            if (auth == null) {
                auth = YTsaurusClientAuth.loadUserAndTokenFromEnvironment();
            }
            if (config == null) {
                config = YTsaurusClientConfig.builder().build();
            }
            if (heavyExecutor == null) {
                heavyExecutor = ForkJoinPool.commonPool();
            }
            if (serializationResolver == null) {
                serializationResolver = DefaultSerializationResolver.getInstance();
            }
            return new DirectYTsaurusClient(this);
        }
    }
}
