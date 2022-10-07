package ru.yandex.yt.ytclient.operations;

import java.util.Iterator;

import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;

public interface Reducer<TInput, TOutput> extends MapperOrReducer<TInput, TOutput> {
    default void reduce(
            Iterator<TInput> input,
            Yield<TOutput> yield,
            Statistics statistics,
            OperationContext context) {
    }
}
