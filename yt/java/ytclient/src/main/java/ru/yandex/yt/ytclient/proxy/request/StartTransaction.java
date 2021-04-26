package ru.yandex.yt.ytclient.proxy.request;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

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
        extends RequestBase<StartTransaction>
        implements HighLevelRequest<TReqStartTransaction.Builder> {
    final TransactionType type;
    final boolean sticky;

    Duration transactionTimeout = Duration.ofSeconds(15);
    @Nullable Instant deadline = null;
    @Nullable GUID id = null;
    @Nullable GUID parentId = null;

    boolean ping = TReqStartTransaction.getDefaultInstance().getPing();
    boolean pingAncestors = TReqStartTransaction.getDefaultInstance().getPingAncestors();

    @Nullable Atomicity atomicity;
    @Nullable Durability durability;
    @Nullable Duration pingPeriod = Duration.ofSeconds(5);

    public StartTransaction(TransactionType type) {
        this(type, type == TransactionType.Tablet);
    }

    private StartTransaction(TransactionType type, boolean sticky) {
        this.type = type;
        this.sticky = sticky;
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
        return type;
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
    public StartTransaction setTransactionTimeout(Duration timeout) {
        this.transactionTimeout = timeout;
        return this;
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
     * Set ping period.
     *
     * If ping period is set yt client will automatically ping transaction with specified period.
     * @see #setTimeout
     */
    public StartTransaction setPingPeriod(@Nullable Duration pingPeriod) {
        this.pingPeriod = pingPeriod;
        return this;
    }

    /**
     * Get ping period.
     */
    public Optional<Duration> getPingPeriod() {
        return Optional.ofNullable(pingPeriod);
    }

    /**
     * Set deadline.
     *
     * If deadline is set transaction will be forcefully aborted upon reaching it.
     */
    public StartTransaction setDeadline(@Nullable Instant deadline) {
        this.deadline = deadline;
        return this;
    }

    /**
     * Get deadline.
     * @see #setDeadline
     */
    public Optional<Instant> getDeadline() {
        return Optional.ofNullable(deadline);
    }

    /**
     * Use specified GUID for newly created transaction.
     * Can only be used with Tablet transactions.
     *
     * If id is not specified, server will assign default value.
     */
    public StartTransaction setId(@Nullable GUID id) {
        this.id = GUID.isEmpty(id) ? null : id;
        return this;
    }

    /**
     * Get GUID to use with transaction being created.
     * @see #setId
     */
    public Optional<GUID> getId() {
        return Optional.ofNullable(id);
    }

    /**
     * Set id of parent transaction.
     */
    public StartTransaction setParentId(@Nullable GUID parentId) {
        this.parentId = GUID.isEmpty(parentId) ? null : parentId;
        return this;
    }

    /**
     * Get id of parent transaction.
     */
    public Optional<GUID> getParentId() {
        return Optional.ofNullable(parentId);
    }

    /**
     * Set atomicity of transaction.
     *
     * If not specified atomicity FULL will be used.
     * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables#atomarnost">
     *     documentation
     *     </a>
     */
    public StartTransaction setAtomicity(@Nullable Atomicity atomicity) {
        this.atomicity = atomicity;
        return this;
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
     * Set durability of transaction.
     *
     * By default durability SYNC is used.
     *
     * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables#sohrannost">
     *     documentation
     *     </a>
     */
    public StartTransaction setDurability(@Nullable Durability durability) {
        this.durability = durability;
        return this;
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

    public StartTransaction setPing(boolean ping) {
        this.ping = ping;
        return this;
    }

    public boolean getPingAncestors() {
        return pingAncestors;
    }

    public StartTransaction setPingAncestors(boolean pingAncestors) {
        this.pingAncestors = pingAncestors;
        return this;
    }

    public boolean getSticky() {
        return sticky;
    }

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
    }

    @Nonnull
    @Override
    protected StartTransaction self() {
        return this;
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
}
