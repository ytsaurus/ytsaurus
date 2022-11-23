package tech.ytsaurus.client.operations;

import java.util.Iterator;

import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;

public interface Mapper<TInput, TOutput> extends MapperOrReducer<TInput, TOutput> {
    default void map(TInput input, Yield<TOutput> yield, Statistics statistics, OperationContext context) {
    }

    default void map(Iterator<TInput> inputIt, Yield<TOutput> yield, Statistics statistics, OperationContext context) {
        while (inputIt.hasNext()) {
            map(inputIt.next(), yield, statistics, context);
        }
    }
}
