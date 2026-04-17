package tech.ytsaurus.client.rows;

import java.util.List;

import javax.persistence.Entity;

import org.junit.Test;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.testlib.proto.ProtoRow;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.Assert.assertEquals;

public class ProtobufFieldEntitySchemaTest {

    @Test
    public void testTableSchema() {
        TableSchema schema = EntityTableSchemaCreator.create(EntityWithProtobuf.class);
        assertEquals(1, schema.getColumns().size());
        ColumnSchema column = schema.getColumns().get(0);
        assertEquals("message", column.getName());
        assertEquals(TiType.optional(TiType.string()), column.getTypeV3());
    }

    @Test
    public void testSkiffSchema() {
        var entitySchema = SchemaConverter.toSkiffSchema(
                EntityTableSchemaCreator.create(EntityWithProtobuf.class)
        );
        SkiffSchema expectedSchema = SkiffSchema.tuple(
                List.of(
                        SkiffSchema.variant8(
                                List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.simpleType(WireType.STRING_32)
                                )
                        ).setName("message")
                )
        );
        assertEquals(expectedSchema, entitySchema);
    }

    // Test Entities.

    @Entity
    static class EntityWithProtobuf {
        private ProtoRow message;
    }

}
