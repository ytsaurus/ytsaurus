package tech.ytsaurus.core;

import java.util.Optional;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class GUIDTest {
    @Test
    public void equals() {
        Assert.assertEquals(new GUID(1, 2, 3, 4), new GUID(1, 2, 3, 4));
        Assert.assertNotEquals(new GUID(1, 2, 3, 4), new GUID(1, 2, 3, 5));
    }

    @Test
    public void hashCodeTest() {
        Assert.assertEquals(new GUID(1, 2, 3, 4).hashCode(), new GUID(1, 2, 3, 4).hashCode());
        Assert.assertNotEquals(new GUID(1, 2, 3, 4).hashCode(), new GUID(1, 2, 3, 5).hashCode());
    }

    @Test
    public void toStringTest() {
        Assert.assertEquals("1-2-3-4", new GUID(1, 2, 3, 4).toString());
        Assert.assertEquals("2f-a-b-9", new GUID(2 * 16 + 15, 10, 11, 9).toString());
    }

    @Test
    public void valueOf() {
        Assert.assertEquals(new GUID(1, 2, 3, 4), GUID.valueOf("1-2-3-4"));
        Assert.assertEquals(new GUID(2 * 16 + 15, 10, 11, 9), GUID.valueOf("2f-a-b-9"));
    }

    @Test
    public void valueOfO() {
        Assert.assertEquals(Optional.of(new GUID(1, 2, 3, 4)), GUID.valueOfO("1-2-3-4"));
        Assert.assertEquals(Optional.empty(), GUID.valueOfO("0-0-0-0"));
    }

    @Test
    public void isValid() {
        Assert.assertTrue(GUID.isValid("1-2-3-4"));
        Assert.assertTrue(GUID.isValid("2f-a-b-9"));
        Assert.assertFalse(GUID.isValid("1-2-3--"));
        Assert.assertFalse(GUID.isValid("1-2-3"));
        Assert.assertFalse(GUID.isValid(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void valueOfIncorrect() {
        GUID.valueOf("1-2-3");
    }

    @Test
    public void toStringCorrect() {
        GUID guid = new GUID(0x0123456789abcdefL, 0x56789abcdef01234L);
        Assert.assertEquals(guid.toString(), "56789abc-def01234-1234567-89abcdef");
    }

    @Test
    public void toStringHighBits() {
        long first = Long.parseUnsignedLong("89abcdef01234567", 16);
        long second = Long.parseUnsignedLong("def0123456789abc", 16);
        GUID guid = new GUID(first, second);
        Assert.assertEquals(guid.toString(), "def01234-56789abc-89abcdef-1234567");
    }

    @Test
    public void fromStringCorrect() {
        GUID expected = new GUID(0x0123456789abcdefL, 0x56789abcdef01234L);
        GUID guid = GUID.valueOf("56789abc-def01234-1234567-89abcdef");
        Assert.assertEquals(guid, expected);
    }

    @Test
    public void fromStringHighBits() {
        long first = Long.parseUnsignedLong("89abcdef01234567", 16);
        long second = Long.parseUnsignedLong("def0123456789abc", 16);
        GUID expected = new GUID(first, second);
        GUID guid = GUID.valueOf("def01234-56789abc-89abcdef-1234567");
        Assert.assertEquals(guid, expected);
    }

    @Test
    public void createWorks() {
        Random random = new Random(12345L);
        GUID guid = GUID.create(random);
        int counter = (int) (guid.getSecond() >>> 32);
        Assert.assertEquals(guid.toString(), Integer.toUnsignedString(counter, 16) + "-eed8a922-5c9f20d5-8361b331");
    }
}
