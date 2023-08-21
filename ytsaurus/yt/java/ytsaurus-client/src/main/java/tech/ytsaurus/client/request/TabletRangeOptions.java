package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.TTabletRangeOptions;

public class TabletRangeOptions {
    private @Nullable
    Integer firstTabletIndex = null;
    private @Nullable
    Integer lastTabletIndex = null;

    public TabletRangeOptions() {
    }

    public TabletRangeOptions(int firstTabletIndex, int lastTabletIndex) {
        this.firstTabletIndex = firstTabletIndex;
        this.lastTabletIndex = lastTabletIndex;
    }

    public TTabletRangeOptions toProto() {
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
