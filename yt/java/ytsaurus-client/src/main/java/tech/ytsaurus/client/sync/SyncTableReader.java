package tech.ytsaurus.client.sync;

import java.io.IOException;
import java.util.Iterator;

public interface SyncTableReader<T> extends Iterator<T>, AutoCloseable {
    @Override
    void close() throws IOException;
}
