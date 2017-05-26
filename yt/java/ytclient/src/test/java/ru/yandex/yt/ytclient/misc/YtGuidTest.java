package ru.yandex.yt.ytclient.misc;

import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class YtGuidTest {
    private static final Logger logger = LoggerFactory.getLogger(YtGuidTest.class);

    @Test
    public void toStringCorrect() {
        YtGuid guid = new YtGuid(0x0123456789abcdefL, 0x56789abcdef01234L);
        assertThat(guid.toString(), is("56789abc-def01234-1234567-89abcdef"));
    }

    @Test
    public void toStringHighBits() {
        long first = Long.parseUnsignedLong("89abcdef01234567", 16);
        long second = Long.parseUnsignedLong("def0123456789abc", 16);
        YtGuid guid = new YtGuid(first, second);
        assertThat(guid.toString(), is("def01234-56789abc-89abcdef-1234567"));
    }

    @Test
    public void fromStringCorrect() {
        YtGuid expected = new YtGuid(0x0123456789abcdefL, 0x56789abcdef01234L);
        YtGuid guid = YtGuid.fromString("56789abc-def01234-1234567-89abcdef");
        assertThat(guid, is(expected));
    }

    @Test
    public void fromStringHighBits() {
        long first = Long.parseUnsignedLong("89abcdef01234567", 16);
        long second = Long.parseUnsignedLong("def0123456789abc", 16);
        YtGuid expected = new YtGuid(first, second);
        YtGuid guid = YtGuid.fromString("def01234-56789abc-89abcdef-1234567");
        assertThat(guid, is(expected));
    }

    @Test
    public void createWorks() {
        Random random = new Random(12345L);
        YtGuid guid = YtGuid.create(random);
        int counter = (int) (guid.getSecond() >>> 32);
        assertThat(guid.toString(), is(Integer.toUnsignedString(counter, 16) + "-eed8a922-5c9f20d5-8361b331"));
    }

    @Test
    public void fromBytes() {
        YtGuid guid = YtGuid.fromBytes(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        assertThat(guid, is(new YtGuid(0x0807060504030201L, 0x100f0e0d0c0b0a09L)));
    }

    @Test
    public void getBytes() {
        YtGuid guid = new YtGuid(0x0807060504030201L, 0x100f0e0d0c0b0a09L);
        byte[] expected = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        assertThat(guid.getBytes(), is(expected));
    }
}
