package ru.yandex.yt.ytclient.request;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.proxy.request.Atomicity;
import ru.yandex.yt.ytclient.proxy.request.Durability;
import ru.yandex.yt.ytclient.proxy.request.TransactionType;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytree.TAttributeDictionary;

/**
 * Request for starting transaction.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#start_tx">
 *     start_tx documentation
 *     </a>
 * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables">
 *     dynamic tables documentation
 *     </a>
 */
@NonNullApi
@NonNullFields
public class StartTransaction
        extends RequestBase<StartTransaction.Builder, StartTransaction>
        implements HighLevelRequest<TReqStartTransaction.Builder> {
    private final TransactionType type;
    private final boolean sticky;

    private final Duration transactionTimeout;
    @Nullable
    private final Instant deadline;
    @Nullable
    private final GUID id;
    @Nullable
    private final GUID parentId;

    private final boolean ping;
    private final boolean pingAncestors;

    @Nullable
    private final Atomicity atomicity;
    @Nullable
    private final Durability durability;
    @Nullable
    private final Duration pingPeriod;
    @Nullable
    private final Duration failedPingRetryPeriod;
    private final Map<String, YTreeNode> attributes;

    @Nullable
    private final Consumer<Exception> onPingFailed;

    public StartTransaction(BuilderBase<?> builder) {
        super(builder);
        this.type = Objects.requireNonNull(builder.type);
        this.sticky = Objects.requireNonNull(builder.sticky);
        this.transactionTimeout = builder.transactionTimeout;
        this.deadline = builder.deadline;
        this.id = builder.id;
        this.parentId = builder.parentId;
        this.ping = builder.ping;
        this.pingAncestors = builder.pingAncestors;
        this.atomicity = builder.atomicity;
        this.durability = builder.durability;
        this.pingPeriod = builder.pingPeriod;
        this.failedPingRetryPeriod = builder.failedPingRetryPeriod;
        this.attributes = new HashMap<>(builder.attributes);
        this.onPingFailed = builder.onPingFailed;
    }

    public StartTransaction(TransactionType type) {
        this(type, type == TransactionType.Tablet);
    }

    private StartTransaction(TransactionType type, boolean sticky) {
        this(builder().setType(type).setSticky(sticky));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create request for starting master transaction.
     *
     * Master transactions are for working with static tables and cypress objects.
     */
    public static StartTransaction master() {
        return new StartTransaction(TransactionType.Master);
    }

    /**
     * Create request for starting tablet transaction.
     *
     * Tablet transactions are for working with dynamic tables.
     */
    public static StartTransaction tablet() {
        return new StartTransaction(TransactionType.Tablet);
    }

    /**
     * Create request for starting sticky master transaction.
     *
     * Such type of transactions can be used to work with all types of objects: cypress / static tables / dynamic tables.
     * Though their usage is discouraged: prefer to use either master or tablet transactions.
     * Compared to tablet transactions they create additional load on masters and have other special effects that you
     * might not want to have.
     */
    public static StartTransaction stickyMaster() {
        return new StartTransaction(TransactionType.Master, true);
    }

    public TransactionType getType() {
        return Objects.requireNonNull(type);
    }

    /**
     * Get transaction timeout.
     *
     * @see #setTimeout
     */
    public Duration getTransactionTimeout() {
        return transactionTimeout;
    }

    /**
     * Get ping period.
     */
    public Optional<Duration> getPingPeriod() {
        return Optional.ofNullable(pingPeriod);
    }

    /**
     * Get failed ping retry period.
     */
    public Optional<Duration> getFailedPingRetryPeriod() {
        return Optional.ofNullable(failedPingRetryPeriod);
    }

    /**
     * Get operation executed on ping failure.
     * @see #setOnPingFailed
     */
    public Optional<Consumer<Exception>> getOnPingFailed() {
        return Optional.ofNullable(onPingFailed);
    }

    /**
     * Get deadline.
     * @see #setDeadline
     */
    public Optional<Instant> getDeadline() {
        return Optional.ofNullable(deadline);
    }

    /**
     * Get GUID to use with transaction being created.
     * @see #setId
     */
    public Optional<GUID> getId() {
        return Optional.ofNullable(id);
    }

    /**
     * Get id of parent transaction.
     */
    public Optional<GUID> getParentId() {
        return Optional.ofNullable(parentId);
    }

    /**
     * Get atomicity of transaction.
     *
     * @see #setAtomicity
     */
    public Optional<Atomicity> getAtomicity() {
        return Optional.ofNullable(atomicity);
    }

    /**
     * @see #setDurability
     */
    public Optional<Durability> getDurability() {
        return Optional.ofNullable(durability);
    }

    public boolean getPing() {
        return ping;
    }


    public boolean getPingAncestors() {
        return pingAncestors;
    }

    public boolean getSticky() {
        return Objects.requireNonNull(sticky);
    }

    public Map<String, YTreeNode> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartTransaction.Builder, ?> builder) {
        builder.body().setType(type.getProtoValue());
        builder.body().setTimeout(ApiServiceUtil.durationToYtMicros(transactionTimeout));

        if (deadline != null) {
            builder.body().setDeadline(ApiServiceUtil.instantToYtMicros(deadline));
        }
        if (id != null) {
            builder.body().setId(RpcUtil.toProto(id));
        }
        if (parentId != null) {
            builder.body().setParentId(RpcUtil.toProto(parentId));
        }
        if (ping != TReqStartTransaction.getDefaultInstance().getPing()) {
            builder.body().setPing(ping);
        }
        if (pingAncestors != TReqStartTransaction.getDefaultInstance().getPingAncestors()) {
            builder.body().setPingAncestors(pingAncestors);
        }
        if (sticky != TReqStartTransaction.getDefaultInstance().getSticky()) {
            builder.body().setSticky(sticky);
        }
        if (atomicity != null) {
            builder.body().setAtomicity(atomicity.getProtoValue());
        }
        if (durability != null) {
            builder.body().setDurability(durability.getProtoValue());
        }
        if (!attributes.isEmpty()) {
            final TAttributeDictionary.Builder attributesBuilder = builder.body().getAttributesBuilder();
            for (Map.Entry<String, YTreeNode> entry : attributes.entrySet()) {
                attributesBuilder.addAttributesBuilder()
                        .setKey(entry.getKey())
                        .setValue(ByteString.copyFrom(entry.getValue().toBinary()));
            }
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Type: ").append(type);
        sb.append("; TransactionTimeout: ").append(transactionTimeout);
        if (parentId != null) {
            sb.append("; ParentId: ").append(parentId);
        }

        if (atomicity != null) {
            sb.append("; Atomicity: ").append(atomicity);
        }
        if (durability != null) {
            sb.append("; Durability: ").append(durability);
        }
        sb.append(";");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setType(type)
                .setSticky(sticky)
                .setTransactionTimeout(transactionTimeout)
                .setDeadline(deadline)
                .setId(id)
                .setParentId(parentId)
                .setPing(ping)
                .setPingAncestors(pingAncestors)
                .setAtomicity(atomicity)
                .setDurability(durability)
                .setPingPeriod(pingPeriod)
                .setFailedPingRetryPeriod(failedPingRetryPeriod)
                .setAttributes(attributes)
                .setOnPingFailed(onPingFailed)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>> extends RequestBase.Builder<TBuilder, StartTransaction> {
        @Nullable
        private TransactionType type;
        @Nullable
        private Boolean sticky;

        private Duration transactionTimeout = Duration.ofSeconds(15);
        @Nullable
        private Instant deadline = null;
        @Nullable
        private GUID id = null;
        @Nullable
        private GUID parentId = null;

        private boolean ping = TReqStartTransaction.getDefaultInstance().getPing();
        private boolean pingAncestors = TReqStartTransaction.getDefaultInstance().getPingAncestors();

        @Nullable
        private Atomicity atomicity;
        @Nullable
        private Durability durability;
        @Nullable
        private Duration pingPeriod = Duration.ofSeconds(5);
        @Nullable
        private Duration failedPingRetryPeriod;
        private final Map<String, YTreeNode> attributes = new HashMap<>();

        @Nullable
        private Consumer<Exception> onPingFailed;

        public TBuilder setType(TransactionType type) {
            this.type = type;
            return self();
        }

        public TBuilder setSticky(boolean sticky) {
            this.sticky = sticky;
            return self();
        }

        /**
         * Set transaction timeout.
         *
         * Transaction is aborted by server if it's not pinged for this specified duration.
         * If it's not specified, then server will use default value of 15 seconds.
         *
         * If you ever change default timeout consider also change ping period.
         * @see #setPingPeriod
         */
        public TBuilder setTransactionTimeout(Duration timeout) {
            this.transactionTimeout = timeout;
            return self();
        }

        /**
         * Set ping period.
         *
         * If ping period is set yt client will automatically ping transaction with specified period.
         * @see #setTimeout
         */
        public TBuilder setPingPeriod(@Nullable Duration pingPeriod) {
            this.pingPeriod = pingPeriod;
            return self();
        }

        /**
         * Set failed ping retry period.
         *
         * If transaction ping fails, it will retry with this period
         * @see #setPingPeriod
         */
        public TBuilder setFailedPingRetryPeriod(@Nullable Duration failedPingRetryPeriod) {
            this.failedPingRetryPeriod = failedPingRetryPeriod;
            return self();
        }

        /**
         * Set operation executed on ping failure
         *
         * @param onPingFailed operation, which will be executed
         */
        public TBuilder setOnPingFailed(@Nullable Consumer<Exception> onPingFailed) {
            this.onPingFailed = onPingFailed;
            return self();
        }

        /**
         * Set deadline.
         *
         * If deadline is set transaction will be forcefully aborted upon reaching it.
         */
        public TBuilder setDeadline(@Nullable Instant deadline) {
            this.deadline = deadline;
            return self();
        }

        /**
         * Use specified GUID for newly created transaction.
         * Can only be used with Tablet transactions.
         *
         * If id is not specified, server will assign default value.
         */
        public TBuilder setId(@Nullable GUID id) {
            this.id = GUID.isEmpty(id) ? null : id;
            return self();
        }

        /**
         * Set id of parent transaction.
         */
        public TBuilder setParentId(@Nullable GUID parentId) {
            this.parentId = GUID.isEmpty(parentId) ? null : parentId;
            return self();
        }

        /**
         * Set atomicity of transaction.
         *
         * If not specified atomicity FULL will be used.
         * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables#atomarnost">
         *     documentation
         *     </a>
         */
        public TBuilder setAtomicity(@Nullable Atomicity atomicity) {
            this.atomicity = atomicity;
            return self();
        }

        /**
         * Set durability of transaction.
         *
         * By default, durability SYNC is used.
         *
         * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables#sohrannost">
         *     documentation
         *     </a>
         */
        public TBuilder setDurability(@Nullable Durability durability) {
            this.durability = durability;
            return self();
        }

        public TBuilder setPing(boolean ping) {
            this.ping = ping;
            return self();
        }

        public TBuilder setPingAncestors(boolean pingAncestors) {
            this.pingAncestors = pingAncestors;
            return self();
        }

        public TBuilder setAttributes(@Nonnull Map<String, YTreeNode> attributes) {
            this.attributes.clear();
            this.attributes.putAll(attributes);
            return self();
        }

        @Override
        public StartTransaction build() {
            return new StartTransaction(this);
        }
    }
}
