package ru.yandex.yt.ytclient.object;

import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;

import static org.hamcrest.MatcherAssert.assertThat;

public class MappedRowSerializerTest {
    @Test
    public void testAsTableSchema() {
        YTreeSerializer serializer = YTreeObjectSerializerFactory.forClass(TableRowKeyField.class);
        var columns = MappedRowSerializer.asTableSchema(serializer.getFieldMap()).getColumns();
        assertThat("Expected two columns", columns.size() == 2);
        assertThat("Key field isn't optional", columns.get(0).getTypeV3().isOptional());
        assertThat("Value isn't optional", columns.get(1).getTypeV3().isOptional());

        serializer = YTreeObjectSerializerFactory.forClass(TableRowNotKeyField.class);
        columns = MappedRowSerializer.asTableSchema(serializer.getFieldMap()).getColumns();
        assertThat("Expected two columns", columns.size() == 2);
        assertThat("Key field isn't optional", columns.get(0).getTypeV3().isOptional());
        assertThat("Value isn't optional", columns.get(1).getTypeV3().isOptional());
    }

    @YTreeObject
    static class TableRowKeyField {
        @YTreeKeyField
        public String field;
        public Integer value;

        public TableRowKeyField(String field, Integer value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TableRowKeyField(\"%s, %d\")", field, value);
        }
    }

    @YTreeObject
    static class TableRowNotKeyField {
        public String field;
        public Integer value;

        public TableRowNotKeyField(String field, Integer value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TableRowNotKeyField(\"%s, %d\")", field, value);
        }
    }
}
