package tech.ytsaurus.flow.row.codec;

import java.util.Objects;

import javax.persistence.Entity;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.TypeAwareProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.TypeAwareProtoWireProtocolWriter;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.typeinfo.TypeInfo;
import tech.ytsaurus.flow.utils.YsonUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that an {@code @Entity} field embedded in another entity is carried as binary YSON
 * over both the typed stream wire ({@link TypeAwareProtoWireProtocolWriter} /
 * {@link TypeAwareProtoWireProtocolReader}) and the raw stream payload
 * ({@link PayloadBuilder} / {@link Payload}).
 */
class EmbeddedEntityRowTest {

    @Entity
    static class Inner {
        String label;
        Integer num;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Inner that = (Inner) o;
            return Objects.equals(label, that.label) && Objects.equals(num, that.num);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, num);
        }
    }

    @Entity
    static class Outer {
        String id;
        Inner inner;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Outer that = (Outer) o;
            return Objects.equals(id, that.id) && Objects.equals(inner, that.inner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, inner);
        }
    }

    private static Inner inner(String label, int num) {
        var inner = new Inner();
        inner.label = label;
        inner.num = num;
        return inner;
    }

    // ---- Typed stream ----

    @Test
    void typedStreamRoundTripsEmbeddedEntity() {
        var outer = new Outer();
        outer.id = "o";
        outer.inner = inner("L", 5);

        var typeInfo = new TypeInfo<>(Outer.class);
        ByteString bytes = new TypeAwareProtoWireProtocolWriter<>(typeInfo).writeEntity(outer);
        Outer decoded = new TypeAwareProtoWireProtocolReader<>(typeInfo).readChunk(bytes);

        assertEquals("o", decoded.id);
        assertEquals(inner("L", 5), decoded.inner);
    }

    @Test
    void typedStreamEncodesEmbeddedEntityAsBinaryYson() {
        var outer = new Outer();
        outer.id = "o";
        outer.inner = inner("L", 5);

        var typeInfo = new TypeInfo<>(Outer.class);
        int innerColumnId = typeInfo.getTableSchema().findColumn("inner");

        ByteString bytes = new TypeAwareProtoWireProtocolWriter<>(typeInfo).writeEntity(outer);
        // The embedded entity column carries exactly the bytes produced by EntityYsonSerializer.
        byte[] expectedYson = new EntityYsonSerializer<>(Inner.class).serialize(inner("L", 5));

        var row = new ProtoWireProtocolReader(bytes).readUnversionedRow();
        byte[] actualYson = row.getValues().get(innerColumnId).bytesValue();
        assertEquals(YsonUtils.yTreeFromBinary(expectedYson), YsonUtils.yTreeFromBinary(actualYson));
    }

    @Test
    void typedStreamRoundTripsNullEmbeddedEntity() {
        var outer = new Outer();
        outer.id = "o";
        outer.inner = null;

        var typeInfo = new TypeInfo<>(Outer.class);
        ByteString bytes = new TypeAwareProtoWireProtocolWriter<>(typeInfo).writeEntity(outer);
        Outer decoded = new TypeAwareProtoWireProtocolReader<>(typeInfo).readChunk(bytes);

        assertEquals("o", decoded.id);
        assertNull(decoded.inner);
    }

    // ---- Raw stream ----

    private static TableSchema rawSchema() {
        return TableSchema.builder()
                .add(new ColumnSchema("id", ColumnValueType.STRING))
                .add(new ColumnSchema("inner", ColumnValueType.COMPOSITE))
                .build();
    }

    @Test
    void rawPayloadRoundTripsEmbeddedEntity() {
        var schema = rawSchema();
        Payload payload = new PayloadBuilder(schema)
                .set("id", "o")
                .set("inner", inner("L", 5))
                .finish();

        assertEquals("o", payload.get("id", String.class));
        assertEquals(inner("L", 5), payload.get("inner", Inner.class));
    }

    @Test
    void rawPayloadStoresEmbeddedEntityAsBinaryYson() {
        var schema = rawSchema();
        Payload payload = new PayloadBuilder(schema)
                .set("inner", inner("L", 5))
                .finish();

        byte[] expectedYson = new EntityYsonSerializer<>(Inner.class).serialize(inner("L", 5));
        byte[] stored = payload.getRow().getValues().get(schema.findColumn("inner")).bytesValue();
        assertEquals(YsonUtils.yTreeFromBinary(expectedYson), YsonUtils.yTreeFromBinary(stored));
    }

    @Test
    void rawPayloadRoundTripsNullEmbeddedEntity() {
        var schema = rawSchema();
        Payload payload = new PayloadBuilder(schema)
                .set("id", "o")
                .set("inner", null)
                .finish();

        assertEquals("o", payload.get("id", String.class));
        assertNull(payload.get("inner", Inner.class));
    }
}
