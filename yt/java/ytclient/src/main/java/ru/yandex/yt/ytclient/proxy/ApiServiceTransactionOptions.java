package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.EDurability;
import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.proxy.request.Atomicity;
import ru.yandex.yt.ytclient.proxy.request.Durability;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;

public class ApiServiceTransactionOptions {
    private final ETransactionType type;
    private Duration timeout;
    private Instant deadline = null;
    private GUID id;
    private GUID parentId;
    private Boolean autoAbort;
    private Boolean ping;
    private Boolean pingAncestors;
    private Boolean sticky;
    private EAtomicity atomicity;
    private EDurability durability;
    private List<GUID> prerequisiteTransactionIds;
    private Duration pingPeriod = Duration.ofSeconds(5);

    public ApiServiceTransactionOptions(ETransactionType type) {
        this.type = Objects.requireNonNull(type);
    }

    public ETransactionType getType() {
        return type;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public Instant getDeadline() {
        return deadline;
    }

    public GUID getId() {
        return id;
    }

    public GUID getParentId() {
        return parentId;
    }

    public Boolean getAutoAbort() {
        return autoAbort;
    }

    public Boolean getPing() {
        return ping;
    }

    public Boolean getPingAncestors() {
        return pingAncestors;
    }

    public Boolean getSticky() {
        return sticky;
    }

    public EAtomicity getAtomicity() {
        return atomicity;
    }

    public EDurability getDurability() {
        return durability;
    }

    public List<GUID> getPrerequisiteTransactionIds() {
        return prerequisiteTransactionIds;
    }

    public Duration getPingPeriod() {
        return pingPeriod;
    }

    public ApiServiceTransactionOptions setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public ApiServiceTransactionOptions setDeadline(Instant deadline) {
        this.deadline = deadline;
        return this;
    }

    public ApiServiceTransactionOptions setId(GUID id) {
        this.id = id;
        return this;
    }

    public ApiServiceTransactionOptions setParentId(GUID parentId) {
        this.parentId = parentId;
        return this;
    }

    public ApiServiceTransactionOptions setAutoAbort(Boolean autoAbort) {
        this.autoAbort = autoAbort;
        return this;
    }

    public ApiServiceTransactionOptions setPing(Boolean ping) {
        this.ping = ping;
        return this;
    }

    public ApiServiceTransactionOptions setPingAncestors(Boolean pingAncestors) {
        this.pingAncestors = pingAncestors;
        return this;
    }

    public ApiServiceTransactionOptions setSticky(Boolean sticky) {
        this.sticky = sticky;
        return this;
    }

    public ApiServiceTransactionOptions setAtomicity(EAtomicity atomicity) {
        this.atomicity = atomicity;
        return this;
    }

    public ApiServiceTransactionOptions setAtomicity(EDurability durability) {
        this.durability = durability;
        return this;
    }

    public ApiServiceTransactionOptions setPrerequisiteTransactionIds(List<GUID> prerequisiteTransactionIds) {
        this.prerequisiteTransactionIds = prerequisiteTransactionIds;
        return this;
    }

    public ApiServiceTransactionOptions setPingPeriod(Duration pingPeriod) {
        this.pingPeriod = pingPeriod;
        return this;
    }

    public StartTransaction toStartTransaction() {
        StartTransaction startTransaction;
        switch (getType()) {
            case TT_MASTER:
                if (getSticky()) {
                    startTransaction = StartTransaction.stickyMaster();
                } else {
                    startTransaction = StartTransaction.master();
                }
                break;
            case TT_TABLET:
                startTransaction = StartTransaction.tablet();
                break;
            default:
                throw new IllegalStateException("Unexpected type of transaction: " + getType());
        }
        if (timeout != null) {
            startTransaction.setTimeout(timeout);
        }
        if (deadline != null) {
            startTransaction.setDeadline(deadline);
        }
        if (id != null && !id.isEmpty()) {
            startTransaction.setId(id);
        }
        if (parentId != null && !parentId.isEmpty()) {
            startTransaction.setParentId(parentId);
        }
        if (ping != null) {
            startTransaction.setPing(ping);
        }
        if (pingAncestors != null) {
            startTransaction.setPingAncestors(pingAncestors);
        }
        if (atomicity != null) {
            switch (atomicity) {
                case A_FULL:
                    startTransaction.setAtomicity(Atomicity.Full);
                    break;
                case A_NONE:
                    startTransaction.setAtomicity(Atomicity.None);
                    break;
                default:
                    break;
            }
        }
        if (durability != null) {
            switch (durability) {
                case D_SYNC:
                    startTransaction.setDurability(Durability.Sync);
                    break;
                case D_ASYNC:
                    startTransaction.setDurability(Durability.Async);
                    break;
                default:
                    break;
            }
        }
        if (prerequisiteTransactionIds != null) {
            throw new RuntimeException("prerequisite_transaction_ids is not supported in RPC proxy API yet");
        }

        return startTransaction;
    }
}
