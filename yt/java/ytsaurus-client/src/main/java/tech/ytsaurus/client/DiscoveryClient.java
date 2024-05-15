package tech.ytsaurus.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.discovery.Discoverer;
import tech.ytsaurus.client.discovery.DiscoveryServiceMethodTable;
import tech.ytsaurus.client.discovery.GetGroupMeta;
import tech.ytsaurus.client.discovery.GroupMeta;
import tech.ytsaurus.client.discovery.Heartbeat;
import tech.ytsaurus.client.discovery.ListGroups;
import tech.ytsaurus.client.discovery.ListGroupsResult;
import tech.ytsaurus.client.discovery.ListMembers;
import tech.ytsaurus.client.discovery.ListMembersResult;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.rpc.DefaultRpcBusClient;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcUtil;

/**
 * Asynchronous client for discovery server.
 * <p>
 * Discovery server keeps actual information about live groups and members.
 * <p>Client supports basic operations, e.g. retrieving information about existing groups, updating members.
 * Every member should periodically report about liveness by heartbeat mechanism.
 * Other members in the group might be discovered by listing methods.
 */
public class DiscoveryClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryClient.class);

    private final BusConnector busConnector;
    private final boolean isBusConnectorOwner;
    private final RpcOptions rpcOptions;
    private final ClientPoolService clientPoolService;

    private DiscoveryClient(Builder builder) {
        this.busConnector = Objects.requireNonNull(builder.busConnector);
        this.isBusConnectorOwner = builder.isBusConnectorOwner;
        this.rpcOptions = Objects.requireNonNullElseGet(builder.rpcOptions, RpcOptions::new);
        RpcClientFactory factory = (hostPort, name) ->
                new DefaultRpcBusClient(this.busConnector, hostPort.toInetSocketAddress(), name);
        this.clientPoolService = ClientPoolService.discoveryClientPoolBuilder()
                .setDiscoverer(Objects.requireNonNull(builder.discoverer))
                .setOptions(this.rpcOptions)
                .setClientFactory(factory)
                .setEventLoop(this.busConnector.eventLoopGroup())
                .setRandom(new Random())
                .build();
        this.clientPoolService.start();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void close() throws IOException {
        if (isBusConnectorOwner) {
            busConnector.close();
        }
    }

    private <RequestMsgBuilder extends MessageLite.Builder, ResponseMsg extends MessageLite,
            RequestType extends HighLevelRequest<RequestMsgBuilder>>
    CompletableFuture<RpcClientResponse<ResponseMsg>>
    invoke(RequestType req, RpcClientRequestBuilder<RequestMsgBuilder, ResponseMsg> builder) {
        logger.debug("Starting request {}; {}",
                builder,
                req.getArgumentsLogString());
        req.writeHeaderTo(builder.header());
        req.writeTo(builder);
        return builder.invokeVia(busConnector.executorService(), this.clientPoolService);
    }

    /**
     * List members in group.
     *
     * @see ListMembers
     */
    public CompletableFuture<ListMembersResult> listMembers(ListMembers req) {
        return RpcUtil.apply(
                invoke(req, DiscoveryServiceMethodTable.LIST_MEMBERS.createRequestBuilder(rpcOptions)),
                response -> new ListMembersResult(response.body()));
    }

    /**
     * Get group's meta.
     *
     * @see GetGroupMeta
     */
    public CompletableFuture<GroupMeta> getGroupMeta(GetGroupMeta req) {
        return RpcUtil.apply(
                invoke(req, DiscoveryServiceMethodTable.GET_GROUP_META.createRequestBuilder(rpcOptions)),
                response -> GroupMeta.fromProto(response.body().getMeta()));
    }

    /**
     * Report about live member. This method is used for new member registration and existing member update.
     *
     * @see Heartbeat
     */
    public CompletableFuture<Void> heartbeat(Heartbeat req) {
        return RpcUtil.apply(
                invoke(req, DiscoveryServiceMethodTable.HEARTBEAT.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Get list of groups with given prefix.
     *
     * @see ListGroups
     */
    public CompletableFuture<ListGroupsResult> listGroups(ListGroups req) {
        return RpcUtil.apply(
                invoke(req, DiscoveryServiceMethodTable.LIST_GROUPS.createRequestBuilder(rpcOptions)),
                response -> new ListGroupsResult(response.body()));
    }

    public static class Builder {
        @Nullable
        private BusConnector busConnector;
        private boolean isBusConnectorOwner = true;
        @Nullable
        private Discoverer discoverer;
        @Nullable
        private RpcOptions rpcOptions;

        private Builder() {
        }

        /**
         * Set BusConnector for Discovery client.
         *
         * <p>
         * Connector will be owned by DiscoveryClient.
         * DiscoveryClient will close it when {@link DiscoveryClient#close()} is called.
         */
        public Builder setOwnBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = true;
            return self();
        }

        /**
         * Set BusConnector for Discovery client.
         *
         * <p>
         * Connector will not be owned by DiscoveryClient. It's user responsibility to close the connector.
         *
         * @see #setOwnBusConnector
         */
        public Builder setSharedBusConnector(BusConnector connector) {
            this.busConnector = connector;
            isBusConnectorOwner = false;
            return self();
        }

        /**
         * Set Discoverer for Discovery client.
         *
         * <p>
         * Discoverer will be used for retrieving discovery servers addresses.
         */
        public Builder setDiscoverer(Discoverer discoverer) {
            this.discoverer = discoverer;
            return self();
        }

        /**
         * Set RPC connection options.
         */
        public Builder setRpcOptions(RpcOptions rpcOptions) {
            this.rpcOptions = rpcOptions;
            return self();
        }

        /**
         * Finally create a client.
         */
        public DiscoveryClient build() {
            return new DiscoveryClient(this);
        }

        protected Builder self() {
            return this;
        }
    }
}
