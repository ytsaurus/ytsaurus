package tech.ytsaurus.flow.internal.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureCollectorTest {
    @Test
    void successfulBodyReturnsItsValueAndCleansUpOnce() throws Exception {
        var calls = new ArrayList<String>();
        assertEquals("value", FailureCollector.callWithCleanup(() -> {
            calls.add("body");
            return "value";
        }, () -> calls.add("cleanup")));
        assertEquals(List.of("body", "cleanup"), calls);
        assertNull(FailureCollector.callWithCleanup(() -> null, () -> { }));
    }

    @Test
    void checkedBodyFailureRemainsPrimary() {
        var bodyFailure = new IOException("body");
        var cleanupFailure = new IllegalStateException("cleanup");
        var thrown = assertThrows(IOException.class, () -> FailureCollector.callWithCleanup(() -> {
            throw bodyFailure;
        }, () -> {
            throw cleanupFailure;
        }));
        assertSame(bodyFailure, thrown);
        assertArrayEquals(new Throwable[]{cleanupFailure}, thrown.getSuppressed());
    }

    @Test
    void fatalCleanupFailureTakesPrecedenceOverBodyFailure() {
        var bodyFailure = new AssertionError("body");
        var cleanupFailure = new OutOfMemoryError("simulated");
        var thrown = assertThrows(OutOfMemoryError.class, () -> FailureCollector.callWithCleanup(() -> {
            throw bodyFailure;
        }, () -> {
            throw cleanupFailure;
        }));
        assertSame(cleanupFailure, thrown);
        assertArrayEquals(new Throwable[]{bodyFailure}, thrown.getSuppressed());
    }

    @Test
    void firstFatalFailureRemainsPrimaryAndAllActionsRun() {
        var calls = new ArrayList<Integer>();
        var first = new OutOfMemoryError("first");
        var second = new StackOverflowError("second");
        var failures = new FailureCollector();
        failures.add(first);
        assertFalse(failures.tryRun(() -> {
            calls.add(1);
            throw second;
        }));
        assertTrue(failures.tryRun(() -> calls.add(2)));
        assertSame(first, assertThrows(OutOfMemoryError.class, failures::throwUncheckedIfAny));
        assertArrayEquals(new Throwable[]{second}, first.getSuppressed());
        assertEquals(List.of(1, 2), calls);
    }

    @Test
    void repeatedFailureDoesNotSuppressItself() {
        var failure = new IllegalStateException("same failure");
        var failures = new FailureCollector();
        failures.add(failure);
        failures.add(failure);
        assertSame(failure, failures.asException());
        assertSame(failure, assertThrows(IllegalStateException.class, failures::throwUncheckedIfAny));
        assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    void cleanupOnlyFailureIsPropagatedUnchanged() {
        var failure = new LinkageError("cleanup");
        assertSame(failure, assertThrows(LinkageError.class, () -> FailureCollector.callWithCleanup(
                () -> 42, () -> {
                    throw failure;
                })));
    }

    @Test
    void emptyCollectorDoesNotThrowAndOrdinaryFailuresAreSuppressed() {
        var failures = new FailureCollector();
        failures.throwUncheckedIfAny();
        var first = new IllegalArgumentException("first");
        var second = new IllegalStateException("second");
        failures.add(first);
        failures.add(second);
        assertSame(first, failures.asException());
        assertArrayEquals(new Throwable[]{second}, first.getSuppressed());
    }
}
