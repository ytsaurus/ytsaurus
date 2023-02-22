package tech.ytsaurus.client;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.skiff.serialization.EntitySkiffDeserializer;
import tech.ytsaurus.skiff.serialization.SkiffParser;

public class TableAttachmentSkiffReader<T> extends TableAttachmentRowsetReader<T> {
    private final EntitySkiffDeserializer<T> deserializer;

    TableAttachmentSkiffReader(Class<T> entityClass) {
        this.deserializer = new EntitySkiffDeserializer<>(entityClass);
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
            deserializedObjects.add(deserializer.deserialize(parser)
                    .orElseThrow(() -> new IllegalStateException("Cannot deserialize object")));
        }

        return deserializedObjects;
    }
}
