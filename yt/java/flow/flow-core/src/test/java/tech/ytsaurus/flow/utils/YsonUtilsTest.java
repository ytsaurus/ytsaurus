package tech.ytsaurus.flow.utils;

import java.util.Objects;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YsonUtilsTest {

    @Entity
    static class WordCountState {
        String word;
        Long count;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WordCountState that = (WordCountState) o;
            return Objects.equals(word, that.word) && Objects.equals(count, that.count);
        }

        @Override
        public int hashCode() {
            return Objects.hash(word, count);
        }
    }

    private static YTreeNode parse(String yson) {
        return YTreeTextSerializer.deserialize(yson);
    }

    @Test
    void entityRoundTrip() {
        var state = new WordCountState();
        state.word = "hello";
        state.count = 1L;

        byte[] bytes = YsonUtils.serializeEntity(state);
        assertEquals(state, YsonUtils.deserializeEntity(bytes, WordCountState.class));
    }

    @Test
    void serializeEntitySupportsFactoryTypes() {
        byte[] bytes = YsonUtils.serializeEntity(42L);
        assertEquals(42L, YsonUtils.deserializeEntity(bytes, Long.class));
    }

    @Test
    void toIntOrDefaultReturnsValueWhenPresent() {
        assertEquals(42, YsonUtils.toIntOrDefault(YTree.integerNode(42), -1));
        assertEquals(7, YsonUtils.toIntOrDefault(parse("7"), -1));
    }

    @Test
    void toIntOrDefaultReturnsDefaultForNullOrEntity() {
        assertEquals(-1, YsonUtils.toIntOrDefault(null, -1));
        assertEquals(-1, YsonUtils.toIntOrDefault(YTree.entityNode(), -1));
        assertEquals(-1, YsonUtils.toIntOrDefault(parse("#"), -1));
    }

    @Test
    void toIntOrDefaultThrowsOnOverflow() {
        YTreeNode node = YTree.integerNode((long) Integer.MAX_VALUE + 1);
        assertThrows(ArithmeticException.class, () -> YsonUtils.toIntOrDefault(node, 0));
    }

    @Test
    void toLongOrDefaultReturnsValueWhenPresent() {
        assertEquals(123L, YsonUtils.toLongOrDefault(YTree.integerNode(123L), -1L));
        assertEquals(Long.MAX_VALUE, YsonUtils.toLongOrDefault(YTree.integerNode(Long.MAX_VALUE), -1L));
    }

    @Test
    void toLongOrDefaultReturnsDefaultForNullOrEntity() {
        assertEquals(-1L, YsonUtils.toLongOrDefault(null, -1L));
        assertEquals(-1L, YsonUtils.toLongOrDefault(YTree.entityNode(), -1L));
    }

    @Test
    void toStringOrDefaultReturnsValueWhenPresent() {
        assertEquals("hello", YsonUtils.toStringOrDefault(YTree.stringNode("hello"), "default"));
        assertEquals("", YsonUtils.toStringOrDefault(YTree.stringNode(""), "default"));
    }

    @Test
    void toStringOrDefaultReturnsDefaultForNullOrEntity() {
        assertEquals("default", YsonUtils.toStringOrDefault(null, "default"));
        assertEquals("default", YsonUtils.toStringOrDefault(YTree.entityNode(), "default"));
        assertNull(YsonUtils.toStringOrDefault(null, null));
        assertNull(YsonUtils.toStringOrDefault(YTree.entityNode(), null));
    }
}
