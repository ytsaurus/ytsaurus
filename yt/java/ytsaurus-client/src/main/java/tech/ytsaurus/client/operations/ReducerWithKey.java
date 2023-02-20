package tech.ytsaurus.client.operations;

import java.util.Iterator;

import javax.annotation.Nullable;

import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public interface ReducerWithKey<TInput, TOutput, TKey> extends Reducer<TInput, TOutput> {

    TKey key(TInput entry);

    default void reduce(TKey key, Iterator<TInput> entries, Yield<TOutput> yield, Statistics statistics) {
    }

    default void reduce(TKey key, Iterator<TInput> entries, Yield<TOutput> yield, Statistics statistics,
                        OperationContext context) {
        reduce(key, entries, yield, statistics);
    }

    default void reduce(Iterator<TInput> entries, Yield<TOutput> yield, Statistics statistics,
                        @Nullable OperationContext context) {
        ReducerWithKeyIterator<TInput, TKey> it = new ReducerWithKeyIterator<>(this::key, entries);
        TKey key = it.nextKey();
        while (key != null) {
            if (context != null) {
                context.setReturnPrevIndexes(true);
            }
            reduce(key, it, yield, statistics, context);
            it.forEachRemaining(tmp -> {
            });
            key = it.nextKey();
        }
    }
}
