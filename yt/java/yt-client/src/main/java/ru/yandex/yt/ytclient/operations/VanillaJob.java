package ru.yandex.yt.ytclient.operations;

import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;

public interface VanillaJob<TOutput> extends MapperOrReducer<YTreeMapNode, TOutput> {

    default int run(Yield<TOutput> yield, Statistics statistics) {
        return run();
    }

    default int run() {
        return 0;
    }

    @Override
    default YTableEntryType<TOutput> outputType() {
        return YTableEntryTypeUtils.resolve(this, 0);
    }
}
