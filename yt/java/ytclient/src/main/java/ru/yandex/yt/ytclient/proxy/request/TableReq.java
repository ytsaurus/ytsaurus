package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class TableReq<T extends TableReq<T>> extends RequestBase<T> {
    protected @Nullable YPath path;
    protected @Nullable String stringPath;
    protected @Nullable MutatingOptions mutatingOptions;
    protected @Nullable TabletRangeOptions tabletRangeOptions;


    public TableReq(YPath path) {
        this.path = path;
        this.stringPath = null;
    }

    /**
     * @deprecated Use {@link #TableReq(YPath path)} instead.
     */
    @Deprecated
    public TableReq(String path) {
        this.path = null;
        this.stringPath = path;
    }

    public T setMutatingOptions(@Nullable MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return self();
    }

    public T setPath(YPath path) {
        this.path = path;
        return self();
    }

    /**
     * @deprecated Use {@link #setPath(YPath path)} instead.
     */
    @Deprecated
    public T setPath(String path) {
        this.stringPath = path;
        return self();
    }

    public String getPath() {
        return (path != null) ? path.toString() : stringPath;
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

        builder.setField(builder.getDescriptorForType().findFieldByName("path"), getPath());

        return builder;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(getPath()).append("; ");
        super.writeArgumentsLogString(sb);
    }
}
