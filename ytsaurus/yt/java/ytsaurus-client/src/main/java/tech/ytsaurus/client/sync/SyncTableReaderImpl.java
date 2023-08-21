package tech.ytsaurus.client.sync;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import tech.ytsaurus.client.AsyncReader;

class SyncTableReaderImpl<T> implements SyncTableReader<T> {
    private final AsyncReader<T> reader;
    private final Executor executor;
    private Iterator<T> iterator;

    private SyncTableReaderImpl(AsyncReader<T> reader) {
        this.reader = reader;
        this.executor = Executors.newCachedThreadPool();
        this.iterator = Collections.emptyIterator();
    }

    static <T> SyncTableReader<T> wrap(AsyncReader<T> reader) {
        return new SyncTableReaderImpl<>(reader);
    }

    @Override
    public T next() {
        if (hasNext()) {
            return iterator.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
        if (!iterator.hasNext()) {
            var rows = reader.next().join();
            if (rows == null) {
                return false;
            }
            this.iterator = rows.iterator();
        }

        return true;
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        reader.acceptAllAsync(action, executor).join();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
