package ru.yandex.yt.ytclient.proxy;

import java.io.Closeable;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.object.IsCompatibleType.typeCompatibleWith;

public class CompoundClientTest {

    @Test
    public void testCompoundClientIsCloseable() {
        // SPYT project uses this interface, don't remove Closeable from CompoundClient, please
        assertThat(CompoundClient.class, typeCompatibleWith(Closeable.class));
    }
}
