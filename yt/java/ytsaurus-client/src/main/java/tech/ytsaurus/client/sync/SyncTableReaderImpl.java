package tech.ytsaurus.client.sync;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import tech.ytsaurus.client.AsyncReader;

class SyncTableReaderImpl<T> implements SyncTableReader<T> {
    private final AsyncReader<T> reader;
    private Iterator<T> iterator;
    private boolean noMoreRows;

    private SyncTableReaderImpl(AsyncReader<T> reader) {
        this.reader = reader;
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
        if (noMoreRows) {
            return false;
        }
        if (iterator.hasNext()) {
            return true;
        }

        var rows = reader.next().join();
        if (rows == null || rows.size() == 0) {
            noMoreRows = true;
            return false;
        }

        this.iterator = rows.iterator();
        return true;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
