package ru.yandex.inside.yt.kosher.tables;

import java.util.Iterator;


import tech.ytsaurus.core.cypress.YPath;
/**
 * @author sankear
 */
public interface RetriableIterator<T> extends Iterator<T> {
    YPath pathForRetry();

    @SuppressWarnings("unchecked")
    default <F> RetriableIterator<F> uncheckedCast() {
        return (RetriableIterator<F>) this;
    }
}
