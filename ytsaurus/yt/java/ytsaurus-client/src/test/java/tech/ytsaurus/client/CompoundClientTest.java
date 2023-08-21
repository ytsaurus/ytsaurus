package tech.ytsaurus.client;

import java.io.Closeable;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CompoundClientTest {

    @Test
    public void testCompoundClientIsCloseable() {
        // SPYT project uses this interface, don't remove Closeable from CompoundClient, please
        assertTrue(Closeable.class.isAssignableFrom(CompoundClient.class));
    }
}
