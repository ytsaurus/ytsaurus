package tech.ytsaurus.client.request;

import tech.ytsaurus.client.TableAttachmentReader;
import tech.ytsaurus.core.cypress.YPath;

public class ReadTableDirect extends ReadTable<byte[]> {
    public ReadTableDirect(YPath path) {
        super(path, new ReadSerializationContext<byte[]>(TableAttachmentReader.byPass()));
    }
}
