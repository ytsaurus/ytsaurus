package tech.ytsaurus.client.operations;

import java.util.Iterator;

import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public interface Reducer<TInput, TOutput> extends MapperOrReducer<TInput, TOutput> {
    default void reduce(
            Iterator<TInput> input,
            Yield<TOutput> yield,
            Statistics statistics,
            OperationContext context) {
    }
}
