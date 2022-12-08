package tech.ytsaurus.core.operations;

import java.util.Iterator;

import tech.ytsaurus.core.cypress.YPath;

public interface RetriableIterator<T> extends Iterator<T> {
    YPath pathForRetry();

    @SuppressWarnings("unchecked")
    default <F> RetriableIterator<F> uncheckedCast() {
        return (RetriableIterator<F>) this;
    }
}
