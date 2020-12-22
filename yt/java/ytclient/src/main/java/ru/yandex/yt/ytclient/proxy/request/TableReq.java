package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class TableReq<T extends TableReq<T>> extends RequestBase<T> {
    protected String path;
    protected @Nullable MutatingOptions mutatingOptions;
    protected @Nullable TabletRangeOptions tabletRangeOptions;

    public TableReq(String path) {
        this.path = path;
    }

    public T setMutatingOptions(@Nullable MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return self();
    }

    public T setPath(String path) {
        this.path = path;
        return self();
    }

    public String getPath() {
        return path;
    }

    public T setTabletRangeOptions(@Nullable TabletRangeOptions opt) {
        this.tabletRangeOptions = opt;
        return self();
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

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; ");
        super.writeArgumentsLogString(sb);
    }
}
