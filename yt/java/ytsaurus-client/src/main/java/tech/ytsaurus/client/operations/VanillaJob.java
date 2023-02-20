package tech.ytsaurus.client.operations;

import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.ysontree.YTreeMapNode;


@NonNullApi
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
