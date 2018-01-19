package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import ru.yandex.yt.rpcproxy.TTabletRangeOptions;

public class TabletRangeOptions {
    private Optional<Integer> firstTabletIndex = Optional.empty();
    private Optional<Integer> lastTabletIndex = Optional.empty();

    TabletRangeOptions() {}

    TabletRangeOptions(int firstTabletIndex, int lastTabletIndex) {
        this.firstTabletIndex = Optional.of(firstTabletIndex);
        this.lastTabletIndex = Optional.of(lastTabletIndex);
    }

    TTabletRangeOptions toProto() {
        TTabletRangeOptions.Builder builder = TTabletRangeOptions.newBuilder();
        firstTabletIndex.ifPresent(x -> builder.setFirstTabletIndex(x));
        lastTabletIndex.ifPresent(x -> builder.setLastTabletIndex(x));
        return builder.build();
    }
}
