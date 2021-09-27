package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;

@NonNullApi
public abstract class GetLikeReq<T extends GetLikeReq<T>> extends TransactionalRequest<T> {
    protected final YPath path;
    @Nullable protected ColumnFilter attributes;
    @Nullable protected Integer maxSize;
    @Nullable protected MasterReadOptions masterReadOptions;
    @Nullable protected SuppressableAccessTrackingOptions suppressableAccessTrackingOptions;

    GetLikeReq(YPath path) {
        this.path = path.justPath();
    }

    protected GetLikeReq(GetLikeReq<?> getLikeReq) {
        super(getLikeReq);
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

    public T setAttributes(@Nullable ColumnFilter cf) {
        this.attributes = cf;
        return self();
    }

    public Optional<Integer> getMaxSize() {
        return Optional.ofNullable(maxSize);
    }

    public T setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return self();
    }

    public Optional<MasterReadOptions> getMasterReadOptions() {
        return Optional.ofNullable(masterReadOptions);
    }

    public T setMasterReadOptions(@Nullable MasterReadOptions mo) {
        this.masterReadOptions = mo;
        return self();
    }

    public Optional<SuppressableAccessTrackingOptions> getSuppressableAccessTrackingOptions() {
        return Optional.ofNullable(suppressableAccessTrackingOptions);
    }

    public T setSuppressableAccessTrackingOptions(@Nullable SuppressableAccessTrackingOptions s) {
        this.suppressableAccessTrackingOptions = s;
        return self();
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
}
