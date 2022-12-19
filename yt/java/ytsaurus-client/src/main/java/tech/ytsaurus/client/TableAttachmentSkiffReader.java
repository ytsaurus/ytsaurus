package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.skiff.deserializer.EntitySkiffDeserializer;
import tech.ytsaurus.skiff.deserializer.SkiffParser;
import tech.ytsaurus.skiff.schema.SkiffSchema;

public class TableAttachmentSkiffReader<T> extends TableAttachmentRowsetReader<T> {
    private final Class<T> objectClass;
    private final SkiffSchema schema;

    TableAttachmentSkiffReader(Class<T> objectClass, SkiffSchema schema) {
        this.objectClass = objectClass;
        this.schema = schema;
    }

    @Override
    protected List<T> parseMergedRow(ByteBuffer buffer, int size) {
        byte[] data = new byte[size];
        buffer.get(data);

        var parser = new SkiffParser(data);
        List<T> deserializedObjects = new ArrayList<>();

        while (parser.hasMoreData()) {
            // only one entity schema is supported
            parser.parseInt16();
            deserializedObjects.add(EntitySkiffDeserializer.deserialize(parser, objectClass, schema)
                    .orElseThrow(() -> new IllegalStateException("Cannot deserialize object")));
        }

        return deserializedObjects;
    }
}
