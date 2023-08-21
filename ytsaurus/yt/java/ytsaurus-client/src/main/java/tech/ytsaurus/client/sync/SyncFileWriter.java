package tech.ytsaurus.client.sync;

import java.io.OutputStream;

public abstract class SyncFileWriter extends OutputStream {
    public abstract void cancel();

    @Override
    public abstract void close();
}
