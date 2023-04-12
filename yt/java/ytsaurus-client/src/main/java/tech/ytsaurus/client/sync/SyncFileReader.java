package tech.ytsaurus.client.sync;

import java.io.InputStream;

public abstract class SyncFileReader extends InputStream {
    /**
     * Returns revision of file node.
     */
    public abstract long revision();

    @Override
    public abstract void close();
}
