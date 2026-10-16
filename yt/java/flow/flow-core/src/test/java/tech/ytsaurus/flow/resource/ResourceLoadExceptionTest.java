package tech.ytsaurus.flow.resource;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ResourceLoadExceptionTest {
    @Test
    void preservesMessageWithoutCause() {
        var failure = new ResourceLoadException("Pool initialization failed");

        assertEquals("Pool initialization failed", failure.getMessage());
        assertNull(failure.getCause());
    }

    @Test
    void preservesMessageAndCause() {
        var cause = new IOException("Connection refused");
        var failure = new ResourceLoadException("Pool initialization failed", cause);

        assertEquals("Pool initialization failed", failure.getMessage());
        assertSame(cause, failure.getCause());
    }
}
