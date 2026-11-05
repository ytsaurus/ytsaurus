package tech.ytsaurus.flow.service;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionMeterTest {
    @Test
    void returnsTheCallbackValueWithItsStats() throws Exception {
        var value = new Object();
        var calls = new AtomicInteger();
        var measured = new ExecutionMeter().measure(() -> {
            calls.incrementAndGet();
            return value;
        });
        assertSame(value, measured.value());
        assertEquals(1, calls.get());
        assertTrue(measured.stats().getAllocatedBytes().toBytes() >= 0);
        assertTrue(measured.stats().getCpuTime().toNanos() >= 0);
    }

    @Test
    void checkedFailuresAndErrorsPropagateUnchanged() {
        var checked = new IOException("checked");
        var fatal = new OutOfMemoryError("simulated");
        var meter = new ExecutionMeter();
        assertSame(checked, assertThrows(IOException.class, () -> meter.measure(() -> {
            throw checked;
        })));
        assertSame(fatal, assertThrows(OutOfMemoryError.class, () -> meter.measure(() -> {
            throw fatal;
        })));
    }
}
