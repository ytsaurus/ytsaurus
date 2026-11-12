package tech.ytsaurus.client.rows;

import org.junit.Test;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class UnversionedValueTest {
    @Test
    public void testCompositeEquality() {
        byte[] firstBytes = YTree.builder().beginList().value("a").endList().build().toBinary();
        byte[] differentBytes = YTree.builder().beginList().value("b").endList().build().toBinary();
        UnversionedValue first = new UnversionedValue(0, ColumnValueType.COMPOSITE, false, firstBytes);
        UnversionedValue same = new UnversionedValue(0, ColumnValueType.COMPOSITE, false, firstBytes.clone());
        UnversionedValue different = new UnversionedValue(0, ColumnValueType.COMPOSITE, false, differentBytes);

        assertEquals(first, same);
        assertEquals(first.hashCode(), same.hashCode());
        assertNotEquals(first, different);
        assertNotEquals(different, first);
    }
}
