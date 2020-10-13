package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;

@NonNullApi
public class GetLikeReq<T extends GetLikeReq<T>> extends RequestBase<T> {
    protected final String path;
    @Nullable protected ColumnFilter attributes;
    @Nullable protected Integer maxSize;
    @Nullable protected TransactionalOptions transactionalOptions;
    @Nullable protected PrerequisiteOptions prerequisiteOptions;
    @Nullable protected MasterReadOptions masterReadOptions;
    @Nullable protected SuppressableAccessTrackingOptions suppressableAccessTrackingOptions;

    public GetLikeReq(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public T setAttributes(@Nullable ColumnFilter cf) {
        this.attributes = cf;
        //noinspection unchecked
        return (T)this;
    }

    public T setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        //noinspection unchecked
        return (T)this;
    }

    public T setTransactionalOptions(@Nullable TransactionalOptions to) {
        this.transactionalOptions = to;
        //noinspection unchecked
        return (T)this;
    }

    public T setPrerequisiteOptions(@Nullable PrerequisiteOptions prerequisiteOptions) {
        this.prerequisiteOptions = prerequisiteOptions;
        //noinspection unchecked
        return (T)this;
    }

    public T setMasterReadOptions(@Nullable MasterReadOptions mo) {
        this.masterReadOptions = mo;
        //noinspection unchecked
        return (T)this;
    }

    public T setSuppressableAccessTrackingOptions(@Nullable SuppressableAccessTrackingOptions s) {
        this.suppressableAccessTrackingOptions = s;
        //noinspection unchecked
        return (T)this;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
         if (transactionalOptions != null) {
            transactionalOptions.writeArgumentsLogString(sb);
         }
        super.writeArgumentsLogString(sb);
    }
}
