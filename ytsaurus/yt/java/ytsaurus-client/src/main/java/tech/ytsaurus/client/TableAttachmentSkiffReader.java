package tech.ytsaurus.client;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.rows.EntitySkiffSerializer;
import tech.ytsaurus.skiff.SkiffParser;

public class TableAttachmentSkiffReader<T> extends TableAttachmentRowsetReader<T> {
    private final EntitySkiffSerializer<T> serializer;

    TableAttachmentSkiffReader(EntitySkiffSerializer<T> serializer) {
        this.serializer = serializer;
    }

    public EntitySkiffSerializer<T> getSerializer() {
        return serializer;
    }

    @Override
    protected List<T> parseMergedRow(ByteBuffer buffer, int size) {
        byte[] data = new byte[size];
        buffer.get(data);

        var parser = new SkiffParser(new ByteArrayInputStream(data));
        List<T> deserializedObjects = new ArrayList<>();

        while (parser.hasMoreData()) {
            // only one entity schema is supported
            parser.parseInt16();
            deserializedObjects.add(serializer.deserialize(parser)
                    .orElseThrow(() -> new IllegalStateException("Cannot deserialize object")));
        }

        return deserializedObjects;
    }
}
