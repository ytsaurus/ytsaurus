package ru.yandex.yt.ytclient.misc;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class YtCrc64Test {
    @Test
    public void correctChecksum() {
        long checksum = YtCrc64.fromBytes("Hello, world!".getBytes());
        assertThat(Long.toUnsignedString(checksum, 16), is("cffd4a79ee0de72b"));
    }
}
