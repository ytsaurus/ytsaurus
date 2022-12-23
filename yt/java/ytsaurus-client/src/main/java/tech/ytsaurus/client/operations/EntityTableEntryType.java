package tech.ytsaurus.client.operations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.core.operations.CloseableIterator;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.skiff.deserializer.EntitySkiffDeserializer;
import tech.ytsaurus.skiff.deserializer.SkiffParser;
import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;
import tech.ytsaurus.skiff.serializer.EntitySkiffSchemaCreator;
import tech.ytsaurus.skiff.serializer.EntitySkiffSerializer;
import tech.ytsaurus.ysontree.YTreeStringNode;

public class EntityTableEntryType<T> implements YTableEntryType<T> {
    private static final byte[] FIRST_TABLE_INDEX = new byte[]{0, 0};
    private final Class<T> entityClass;
    private final SkiffSchema entitySchema;
    private final boolean trackIndices;
    private final boolean isInputType;

    public EntityTableEntryType(Class<T> entityClass, boolean trackIndices, boolean isInputType) {
        this.entityClass = entityClass;
        this.entitySchema = EntitySkiffSchemaCreator.getEntitySchema(entityClass);
        if (trackIndices) {
            this.entitySchema.getChildren().add(
                    SkiffSchema.variant8(List.of(
                                    SkiffSchema.nothing(),
                                    SkiffSchema.simpleType(WireType.INT_64)
                            ))
                            .setName("$row_index")
            );
        }
        this.trackIndices = trackIndices;
        this.isInputType = isInputType;
    }

    @Override
    public YTreeStringNode format(FormatContext context) {
        int tableCount = isInputType ?
                context.getInputTableCount().orElseThrow(IllegalArgumentException::new) :
                context.getOutputTableCount().orElseThrow(IllegalArgumentException::new);
        return Format.skiff(entitySchema, tableCount).toTree().stringNode();
    }

    @Override
    public CloseableIterator<T> iterator(InputStream input, OperationContext context) {
        context.withSettingIndices(trackIndices, trackIndices);
        var parser = new SkiffParser(input);
        return new CloseableIterator<>() {
            long rowIndex = 0;

            @Override
            public boolean hasNext() {
                return parser.hasMoreData();
            }

            @Override
            public T next() {
                short tableIndex = parser.parseInt16();
                T object = EntitySkiffDeserializer.deserialize(parser, entityClass, entitySchema)
                        .orElseThrow(NoSuchElementException::new);
                if (trackIndices) {
                    rowIndex++;
                    if (parser.parseVariant8Tag() != 0) {
                        rowIndex = parser.parseInt64();
                    }
                    context.setRowIndex(rowIndex);
                    context.setTableIndex(tableIndex);
                }
                return object;
            }

            @Override
            public void close() throws Exception {
                input.close();
            }

        };
    }

    @Override
    public Yield<T> yield(OutputStream[] output) {
        return new Yield<>() {
            private final EntitySkiffSerializer<T> skiffSerializer = new EntitySkiffSerializer<>(entitySchema);

            @Override
            public void yield(int index, T value) {
                try {
                    output[index].write(FIRST_TABLE_INDEX);
                    output[index].write(skiffSerializer.serialize(value));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close() throws IOException {
                for (OutputStream outputStream : output) {
                    outputStream.close();
                }
            }
        };
    }
}
