package ru.yandex.yt.ytclient.proxy.request;

public class TableReq<T extends TableReq<T>> extends RequestBase<T> {
    protected String path;
    protected MutatingOptions mutatingOptions;
    protected TabletRangeOptions tabletRangeOptions;

    public TableReq(String path) {
        this.path = path;
    }

    public T setMutatingOptions(MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return (T)this;
    }

    public T setPath(String path) {
        this.path = path;
        return (T)this;
    }

    public T setTabletRangeOptions(TabletRangeOptions opt) {
        this.tabletRangeOptions = opt;
        return (T)this;
    }

    public
    <R extends com.google.protobuf.GeneratedMessageV3.Builder<R>>
    R writeTo(R builder) {
        if (tabletRangeOptions != null) {
            builder.setField(
                    builder.getDescriptorForType().findFieldByName("tablet_range_options"),
                    tabletRangeOptions.toProto());
        }

        if (mutatingOptions != null) {
            builder.setField(
                    builder.getDescriptorForType().findFieldByName("mutating_options"),
                    mutatingOptions.toProto());
        }

        builder.setField(builder.getDescriptorForType().findFieldByName("path"), path);

        return builder;
    }
}
