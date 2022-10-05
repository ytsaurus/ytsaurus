package ru.yandex.inside.yt.kosher.tables;

import java.util.Iterator;

import ru.yandex.inside.yt.kosher.cypress.YPath;

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
