package tech.ytsaurus.client;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import tech.ytsaurus.client.request.Atomicity;
import tech.ytsaurus.client.request.Durability;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.EAtomicity;
import tech.ytsaurus.rpcproxy.EDurability;
import tech.ytsaurus.rpcproxy.ETransactionType;
import tech.ytsaurus.ysontree.YTreeNode;

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
    private Duration failedPingRetryPeriod;
    private final Map<String, YTreeNode> attributes = new HashMap<>();
    private Consumer<Exception> onPingFailed;

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

    public Duration getFailedPingRetryPeriod() {
        return failedPingRetryPeriod;
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

    public ApiServiceTransactionOptions setFailedPingRetryPeriod(Duration failedPingRetryPeriod) {
        this.failedPingRetryPeriod = failedPingRetryPeriod;
        return this;
    }

    public ApiServiceTransactionOptions setOnPingFailed(Consumer<Exception> onPingFailed) {
        this.onPingFailed = onPingFailed;
        return this;
    }

    public ApiServiceTransactionOptions setAttributes(@Nonnull Map<String, YTreeNode> attributes) {
        this.attributes.clear();
        this.attributes.putAll(attributes);
        return this;
    }

    public Map<String, YTreeNode> getAttributes() {
        return Collections.unmodifiableMap(attributes);
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
        StartTransaction.Builder startTransactionBuilder;
        switch (getType()) {
            case TT_MASTER:
                if (getSticky()) {
                    startTransactionBuilder = StartTransaction.stickyMaster().toBuilder();
                } else {
                    startTransactionBuilder = StartTransaction.master().toBuilder();
                }
                break;
            case TT_TABLET:
                startTransactionBuilder = StartTransaction.tablet().toBuilder();
                break;
            default:
                throw new IllegalStateException("Unexpected type of transaction: " + getType());
        }
        if (timeout != null) {
            startTransactionBuilder.setTimeout(timeout);
        }
        if (deadline != null) {
            startTransactionBuilder.setDeadline(deadline);
        }
        if (id != null && !id.isEmpty()) {
            startTransactionBuilder.setId(id);
        }
        if (parentId != null && !parentId.isEmpty()) {
            startTransactionBuilder.setParentId(parentId);
        }
        if (ping != null) {
            startTransactionBuilder.setPing(ping);
        }
        if (pingAncestors != null) {
            startTransactionBuilder.setPingAncestors(pingAncestors);
        }
        if (pingPeriod != null) {
            startTransactionBuilder.setPingPeriod(pingPeriod);
        }
        if (failedPingRetryPeriod != null) {
            startTransactionBuilder.setFailedPingRetryPeriod(failedPingRetryPeriod);
        }
        if (atomicity != null) {
            switch (atomicity) {
                case A_FULL:
                    startTransactionBuilder.setAtomicity(Atomicity.Full);
                    break;
                case A_NONE:
                    startTransactionBuilder.setAtomicity(Atomicity.None);
                    break;
                default:
                    break;
            }
        }
        if (durability != null) {
            switch (durability) {
                case D_SYNC:
                    startTransactionBuilder.setDurability(Durability.Sync);
                    break;
                case D_ASYNC:
                    startTransactionBuilder.setDurability(Durability.Async);
                    break;
                default:
                    break;
            }
        }
        if (!attributes.isEmpty()) {
            startTransactionBuilder.setAttributes(getAttributes());
        }
        if (onPingFailed != null) {
            startTransactionBuilder.setOnPingFailed(onPingFailed);
        }
        if (prerequisiteTransactionIds != null) {
            throw new RuntimeException("prerequisite_transaction_ids is not supported in RPC proxy API yet");
        }

        return startTransactionBuilder.build();
    }
}
