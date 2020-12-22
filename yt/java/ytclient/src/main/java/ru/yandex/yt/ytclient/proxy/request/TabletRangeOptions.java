package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TTabletRangeOptions;

@NonNullApi
@NonNullFields
public class TabletRangeOptions {
    private @Nullable Integer firstTabletIndex = null;
    private @Nullable Integer lastTabletIndex = null;

    public TabletRangeOptions() {}

    public TabletRangeOptions(int firstTabletIndex, int lastTabletIndex) {
        this.firstTabletIndex = firstTabletIndex;
        this.lastTabletIndex = lastTabletIndex;
    }

    TTabletRangeOptions toProto() {
        TTabletRangeOptions.Builder builder = TTabletRangeOptions.newBuilder();
        if (firstTabletIndex != null) {
            builder.setFirstTabletIndex(firstTabletIndex);
        }
        if (lastTabletIndex != null) {
            builder.setLastTabletIndex(firstTabletIndex);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
