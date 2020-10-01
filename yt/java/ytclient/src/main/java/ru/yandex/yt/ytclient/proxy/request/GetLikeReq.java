package ru.yandex.yt.ytclient.proxy.request;

public class GetLikeReq<T extends GetLikeReq<T>> extends RequestBase<T> {
    protected final String path;
    protected ColumnFilter attributes;
    protected Integer maxSize;
    protected TransactionalOptions transactionalOptions;
    protected PrerequisiteOptions prerequisiteOptions;
    protected MasterReadOptions masterReadOptions;
    protected SuppressableAccessTrackingOptions suppressableAccessTrackingOptions;

    public GetLikeReq(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public T setAttributes(ColumnFilter cf) {
        this.attributes = cf;
        return (T)this;
    }

    public T setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return (T)this;
    }

    public T setTransactionalOptions(TransactionalOptions to) {
        this.transactionalOptions = to;
        return (T)this;
    }

    public T setPrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
        this.prerequisiteOptions = prerequisiteOptions;
        return (T)this;
    }

    public T setMasterReadOptions(MasterReadOptions mo) {
        this.masterReadOptions = mo;
        return (T)this;
    }

    public T setSuppressableAccessTrackingOptions(SuppressableAccessTrackingOptions s) {
        this.suppressableAccessTrackingOptions = s;
        return (T)this;
    }
}
