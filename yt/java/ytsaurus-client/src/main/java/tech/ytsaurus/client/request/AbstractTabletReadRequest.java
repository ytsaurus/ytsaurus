package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.YtTimestamp;

/**
 * Base class for requests using TTabletReadOptionsBase.
 */
public abstract class AbstractTabletReadRequest<
        TBuilder extends AbstractTabletReadRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractTabletReadRequest<TBuilder, TRequest>> extends RequestBase<TBuilder, TRequest> {

    @Nullable
    protected final YtTimestamp timestamp;
    @Nullable
    protected final YtTimestamp retentionTimestamp;
    @Nullable
    protected final ReplicaConsistency replicaConsistency;

    protected AbstractTabletReadRequest(Builder<?, ?> builder) {
        super(builder);
        this.timestamp = builder.timestamp;
        this.retentionTimestamp = builder.retentionTimestamp;
        this.replicaConsistency = builder.replicaConsistency;
    }

    /**
     * Get timestamp parameter.
     *
     * @see Builder#setTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    /**
     * Get retention-timestamp parameter.
     *
     * @see Builder#setRetentionTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    /**
     * Get replica-consistency parameter.
     *
     * @see Builder#setReplicaConsistency(ReplicaConsistency)
     */
    public Optional<ReplicaConsistency> getReplicaConsistency() {
        return Optional.ofNullable(replicaConsistency);
    }

    /**
     * Base class for builders of requests using TTabletReadOptionsBase parameters.
     */
    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends AbstractTabletReadRequest<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {

        @Nullable
        private YtTimestamp timestamp;
        @Nullable
        private YtTimestamp retentionTimestamp;
        @Nullable
        private ReplicaConsistency replicaConsistency;

        /**
         * Construct empty builder.
         */
        public Builder() {
        }

        /**
         * Set version of a table to be used for lookup request.
         */
        public TBuilder setTimestamp(@Nullable YtTimestamp timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        /**
         * Set lower boundary for value timestamps to be returned.
         * I.e. values that were written before this timestamp are ignored and not returned.
         */
        public TBuilder setRetentionTimestamp(@Nullable YtTimestamp retentionTimestamp) {
            this.retentionTimestamp = retentionTimestamp;
            return self();
        }

        /**
         * Set requested read consistency for chaos replicas.
         */
        public TBuilder setReplicaConsistency(@Nullable ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return self();
        }

        /**
         * Get value of timestamp parameter.
         *
         * @see #setTimestamp parameter
         */
        public Optional<YtTimestamp> getTimestamp() {
            return Optional.ofNullable(timestamp);
        }

        /**
         * Get value of retention-timestamp parameter.
         *
         * @see #setRetentionTimestamp
         */
        public Optional<YtTimestamp> getRetentionTimestamp() {
            return Optional.ofNullable(retentionTimestamp);
        }

        /**
         * Get value of requested read consistency for chaos replicas.
         *
         * @see #setReplicaConsistency
         */
        public Optional<ReplicaConsistency> getReplicaConsistency() {
            return Optional.ofNullable(replicaConsistency);
        }

    }
}
