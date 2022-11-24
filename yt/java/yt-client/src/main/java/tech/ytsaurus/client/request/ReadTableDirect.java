package tech.ytsaurus.client.request;

import tech.ytsaurus.client.TableAttachmentReader;
import tech.ytsaurus.core.cypress.YPath;

public class ReadTableDirect extends ReadTable<byte[]> {
    public ReadTableDirect(YPath path) {
        super(path, new SerializationContext<>(TableAttachmentReader.BYPASS));
    }

    /**
     * @deprecated Use {@link #ReadTableDirect(YPath path)} instead.
     */
    @Deprecated
    public ReadTableDirect(String path) {
        super(ReadTable.<byte[]>builder().setPath(path).setSerializationContext(
                new SerializationContext<>(TableAttachmentReader.BYPASS)));
    }
}
