package tech.ytsaurus.client.operations;

import java.util.Iterator;

import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public interface Mapper<TInput, TOutput> extends MapperOrReducer<TInput, TOutput> {
    default void map(TInput input, Yield<TOutput> yield, Statistics statistics, OperationContext context) {
    }

    default void map(Iterator<TInput> inputIt, Yield<TOutput> yield, Statistics statistics, OperationContext context) {
        while (inputIt.hasNext()) {
            map(inputIt.next(), yield, statistics, context);
        }
    }
}
