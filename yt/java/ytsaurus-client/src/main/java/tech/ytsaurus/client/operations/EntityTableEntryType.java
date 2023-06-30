package tech.ytsaurus.client.operations;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.NoSuchElementException;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.client.rows.EntitySkiffSerializer;
import tech.ytsaurus.client.rows.EntityTableSchemaCreator;
import tech.ytsaurus.client.rows.SchemaConverter;
import tech.ytsaurus.core.operations.CloseableIterator;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.SkiffParser;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.SkiffSerializer;
import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.ysontree.YTreeStringNode;

public class EntityTableEntryType<T> implements YTableEntryType<T> {
    private static final byte[] FIRST_TABLE_INDEX = new byte[]{0, 0};
    private final Class<T> entityClass;
    private final SkiffSchema entitySchema;
    private final TableSchema tableSchema;
    private final boolean trackIndices;
    private final boolean isInputType;

    public EntityTableEntryType(Class<T> entityClass, boolean trackIndices, boolean isInputType) {
        this.entityClass = entityClass;
        this.tableSchema = EntityTableSchemaCreator.create(entityClass, null);
        this.entitySchema = SchemaConverter.toSkiffSchema(tableSchema);
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
            private final EntitySkiffSerializer<T> skiffSerializer =
                    new EntitySkiffSerializer<>(entityClass);
            long rowIndex = 0;
            short tableIndex = 0;

            @Override
            public boolean hasNext() {
                return parser.hasMoreData();
            }

            @Override
            public T next() {
                tableIndex = parser.parseInt16();
                var object = skiffSerializer.deserialize(parser)
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
        var skiffSerializers = new SkiffSerializer[output.length];
        for (int i = 0; i < output.length; i++) {
            skiffSerializers[i] = new SkiffSerializer(new BufferedOutputStream(output[i], 1 << 16));
        }
        return new Yield<>() {
            private final EntitySkiffSerializer<T> entitySerializer =
                    new EntitySkiffSerializer<>(entityClass);

            @Override
            public void yield(int index, T value) {
                skiffSerializers[index].write(FIRST_TABLE_INDEX);
                entitySerializer.serialize(value, skiffSerializers[index]);
            }

            @Override
            public void close() throws IOException {
                for (var serializer : skiffSerializers) {
                    serializer.flush();
                    serializer.close();
                }
            }
        };
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }
}
