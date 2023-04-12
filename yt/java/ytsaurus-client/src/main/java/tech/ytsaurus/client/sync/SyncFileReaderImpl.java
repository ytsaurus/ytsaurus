package tech.ytsaurus.client.sync;

import java.io.IOException;

import tech.ytsaurus.client.FileReader;

class SyncFileReaderImpl extends SyncFileReader {
    private final FileReader reader;
    private byte[] buf;
    private int pos;

    private SyncFileReaderImpl(FileReader reader) {
        this.reader = reader;
        this.buf = new byte[0];
    }

    static SyncFileReader wrap(FileReader reader) {
        return new SyncFileReaderImpl(reader);
    }

    @Override
    public int read() throws IOException {
        if (pos == buf.length) {
            reader.readyEvent().join();
            try {
                if (reader.canRead()) {
                    this.buf = reader.read();
                    this.pos = 0;
                } else {
                    this.buf = new byte[0];
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return buf.length != 0 ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public int available() throws IOException {
        return buf.length - pos;
    }

    @Override
    public long revision() {
        try {
            return reader.revision();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        reader.close().join();
    }
}
