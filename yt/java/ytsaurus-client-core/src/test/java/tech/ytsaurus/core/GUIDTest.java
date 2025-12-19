package tech.ytsaurus.core;

import java.util.Optional;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GUIDTest {
    @Test
    public void equals() {
        assertEquals(new GUID(1, 2, 3, 4), new GUID(1, 2, 3, 4));
        assertNotEquals(new GUID(1, 2, 3, 4), new GUID(1, 2, 3, 5));
    }

    @Test
    public void hashCodeTest() {
        assertEquals(new GUID(1, 2, 3, 4).hashCode(), new GUID(1, 2, 3, 4).hashCode());
        assertNotEquals(new GUID(1, 2, 3, 4).hashCode(), new GUID(1, 2, 3, 5).hashCode());
    }

    @Test
    public void toStringTest() {
        assertEquals("1-2-3-4", new GUID(1, 2, 3, 4).toString());
        assertEquals("2f-a-b-9", new GUID(2 * 16 + 15, 10, 11, 9).toString());
    }

    @Test
    public void valueOf() {
        assertEquals(new GUID(1, 2, 3, 4), GUID.valueOf("1-2-3-4"));
        assertEquals(new GUID(2 * 16 + 15, 10, 11, 9), GUID.valueOf("2f-a-b-9"));
    }

    @Test
    public void valueOfO() {
        assertEquals(Optional.of(new GUID(1, 2, 3, 4)), GUID.valueOfO("1-2-3-4"));
        assertEquals(Optional.empty(), GUID.valueOfO("0-0-0-0"));
    }

    @Test
    public void isValid() {
        assertTrue(GUID.isValid("1-2-3-4"));
        assertTrue(GUID.isValid("2f-a-b-9"));
        assertFalse(GUID.isValid("1-2-3--"));
        assertFalse(GUID.isValid("1-2-3"));
        assertFalse(GUID.isValid(""));
    }

    @Test
    public void valueOfIncorrect() {
        assertThrows(IllegalArgumentException.class, () -> GUID.valueOf("1-2-3"));
    }

    @Test
    public void toStringCorrect() {
        GUID guid = new GUID(0x0123456789abcdefL, 0x56789abcdef01234L);
        assertEquals("56789abc-def01234-1234567-89abcdef", guid.toString());
    }

    @Test
    public void toStringHighBits() {
        long first = Long.parseUnsignedLong("89abcdef01234567", 16);
        long second = Long.parseUnsignedLong("def0123456789abc", 16);
        GUID guid = new GUID(first, second);
        assertEquals("def01234-56789abc-89abcdef-1234567", guid.toString());
    }

    @Test
    public void fromStringCorrect() {
        GUID expected = new GUID(0x0123456789abcdefL, 0x56789abcdef01234L);
        GUID guid = GUID.valueOf("56789abc-def01234-1234567-89abcdef");
        assertEquals(guid, expected);
    }

    @Test
    public void fromStringHighBits() {
        long first = Long.parseUnsignedLong("89abcdef01234567", 16);
        long second = Long.parseUnsignedLong("def0123456789abc", 16);
        GUID expected = new GUID(first, second);
        GUID guid = GUID.valueOf("def01234-56789abc-89abcdef-1234567");
        assertEquals(guid, expected);
    }

    @Test
    public void createWorks() {
        Random random = new Random(12345L);
        GUID guid = GUID.create(random);
        int counter = (int) (guid.getSecond() >>> 32);
        assertEquals(guid.toString(), Integer.toUnsignedString(counter, 16) + "-eed8a922-5c9f20d5-8361b331");
    }
}
