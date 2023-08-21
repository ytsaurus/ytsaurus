package tech.ytsaurus.client.request;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBuilder;

public abstract class GetLikeReq<
        TBuilder extends RequestBase.Builder<TBuilder, TRequest>,
        TRequest extends RequestBase<TBuilder, TRequest>> extends TransactionalRequest<TBuilder, TRequest> {
    protected YPath path;
    @Nullable
    protected ColumnFilter attributes;
    @Nullable
    protected Integer maxSize;
    @Nullable
    protected MasterReadOptions masterReadOptions;
    @Nullable
    protected SuppressableAccessTrackingOptions suppressableAccessTrackingOptions;

    GetLikeReq(Builder<?, ?> builder) {
        super(builder);
        Objects.requireNonNull(builder.path);
        this.path = builder.path;
        this.attributes = builder.attributes;
        this.maxSize = builder.maxSize;
        this.masterReadOptions = builder.masterReadOptions;
        this.suppressableAccessTrackingOptions = builder.suppressableAccessTrackingOptions;
    }

    protected GetLikeReq(GetLikeReq<?, ?> getLikeReq) {
        super(getLikeReq);
        Objects.requireNonNull(getLikeReq.path);
        path = getLikeReq.path;
        attributes = getLikeReq.attributes;
        maxSize = getLikeReq.maxSize;
        masterReadOptions = (getLikeReq.masterReadOptions != null)
                ? new MasterReadOptions(getLikeReq.masterReadOptions)
                : null;
        suppressableAccessTrackingOptions = (getLikeReq.suppressableAccessTrackingOptions != null)
                ? new SuppressableAccessTrackingOptions(getLikeReq.suppressableAccessTrackingOptions)
                : null;
    }

    public YPath getPath() {
        return path;
    }

    public Optional<ColumnFilter> getAttributes() {
        return Optional.ofNullable(attributes);
    }

    public Optional<Integer> getMaxSize() {
        return Optional.ofNullable(maxSize);
    }

    public Optional<MasterReadOptions> getMasterReadOptions() {
        return Optional.ofNullable(masterReadOptions);
    }

    public Optional<SuppressableAccessTrackingOptions> getSuppressableAccessTrackingOptions() {
        return Optional.ofNullable(suppressableAccessTrackingOptions);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; ");
        if (attributes != null) {
            sb.append("Attributes: ").append(attributes).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("path").apply(path::toTree)
                .when(masterReadOptions != null, b -> b.key("read_from").apply(masterReadOptions::toTree))
                .when(attributes != null && attributes.isPresent(),
                        b2 -> b2.key("attributes").apply(attributes::toTree));
    }

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends TransactionalRequest<?, TRequest>>
            extends TransactionalRequest.Builder<TBuilder, TRequest> {
        @Nullable
        protected YPath path;
        @Nullable
        protected ColumnFilter attributes;
        @Nullable
        protected Integer maxSize;
        @Nullable
        protected MasterReadOptions masterReadOptions;
        @Nullable
        protected SuppressableAccessTrackingOptions suppressableAccessTrackingOptions;

        Builder() {
        }

        protected Builder(Builder<?, ?> builder) {
            super(builder);
            path = builder.path;
            attributes = builder.attributes;
            maxSize = builder.maxSize;
            masterReadOptions = (builder.masterReadOptions != null)
                    ? new MasterReadOptions(builder.masterReadOptions)
                    : null;
            suppressableAccessTrackingOptions = (builder.suppressableAccessTrackingOptions != null)
                    ? new SuppressableAccessTrackingOptions(builder.suppressableAccessTrackingOptions)
                    : null;
        }

        public TBuilder setPath(YPath path) {
            this.path = path.justPath();
            return self();
        }

        public TBuilder setAttributes(@Nullable ColumnFilter cf) {
            this.attributes = cf;
            return self();
        }

        public TBuilder setMaxSize(@Nullable Integer maxSize) {
            this.maxSize = maxSize;
            return self();
        }

        public TBuilder setMasterReadOptions(@Nullable MasterReadOptions mo) {
            this.masterReadOptions = mo;
            return self();
        }

        public TBuilder setSuppressableAccessTrackingOptions(@Nullable SuppressableAccessTrackingOptions s) {
            this.suppressableAccessTrackingOptions = s;
            return self();
        }

        public YPath getPath() {
            Objects.requireNonNull(path);
            return path;
        }

        public Optional<ColumnFilter> getAttributes() {
            return Optional.ofNullable(attributes);
        }

        public Optional<Integer> getMaxSize() {
            return Optional.ofNullable(maxSize);
        }

        public Optional<MasterReadOptions> getMasterReadOptions() {
            return Optional.ofNullable(masterReadOptions);
        }

        public Optional<SuppressableAccessTrackingOptions> getSuppressableAccessTrackingOptions() {
            return Optional.ofNullable(suppressableAccessTrackingOptions);
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            sb.append("Path: ").append(path).append("; ");
            if (attributes != null) {
                sb.append("Attributes: ").append(attributes).append("; ");
            }
            super.writeArgumentsLogString(sb);
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            Objects.requireNonNull(path);

            return builder
                    .apply(super::toTree)
                    .key("path").apply(path::toTree)
                    .when(masterReadOptions != null, b -> b.key("read_from").apply(masterReadOptions::toTree))
                    .when(attributes != null && attributes.isPresent(),
                            b2 -> b2.key("attributes").apply(attributes::toTree));
        }
    }
}
