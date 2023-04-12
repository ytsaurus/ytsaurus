package tech.ytsaurus.client.sync;

import java.io.IOException;

import tech.ytsaurus.client.FileWriter;

class SyncFileWriterImpl extends SyncFileWriter {
    private final FileWriter writer;

    private SyncFileWriterImpl(FileWriter writer) {
        this.writer = writer;
    }

    static SyncFileWriter wrap(FileWriter writer) {
        return new SyncFileWriterImpl(writer);
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) (b & 0xFF)});
    }

    @Override
    public void write(byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    @Override
    public void write(byte[] data, int offset, int len) throws IOException {
        if (!writer.write(data, offset, len)) {
            throw new IOException();
        }
        writer.readyEvent().join();
    }

    @Override
    public void close() {
        writer.close().join();
    }

    @Override
    public void cancel() {
        writer.cancel();
    }
}
