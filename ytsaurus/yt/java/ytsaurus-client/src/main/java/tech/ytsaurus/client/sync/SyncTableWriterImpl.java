package tech.ytsaurus.client.sync;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.AsyncWriter;

class SyncTableWriterImpl<T> implements SyncTableWriter<T> {
    private static final int BUFFER_SIZE = 1 << 10;
    private final AsyncWriter<T> writer;
    private final List<T> buf;

    private SyncTableWriterImpl(AsyncWriter<T> writer) {
        this.writer = writer;
        this.buf = new ArrayList<>(BUFFER_SIZE);
    }

    static <T> SyncTableWriter<T> wrap(AsyncWriter<T> writer) {
        return new SyncTableWriterImpl<>(writer);
    }

    @Override
    public void accept(T row) {
        if (buf.size() == BUFFER_SIZE) {
            writer.write(buf).join();
            buf.clear();
        }
        buf.add(row);
    }

    @Override
    public void flush() {
        writer.write(buf).join();
        buf.clear();
    }

    @Override
    public void close() {
        flush();
        writer.finish().join();
    }

    @Override
    public void cancel() {
        writer.cancel();
    }
}
