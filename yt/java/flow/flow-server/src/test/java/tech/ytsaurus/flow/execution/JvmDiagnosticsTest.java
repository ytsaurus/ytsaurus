package tech.ytsaurus.flow.execution;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JvmDiagnosticsTest {

    @Test
    void acceptsBothRecommendedHeapDumpOptions() {
        assertTrue(JvmDiagnostics.hasRecommendedHeapDumpOptions(List.of(
                "-Xmx1g",
                "-XX:+HeapDumpOnOutOfMemoryError",
                "-XX:HeapDumpPath=/tmp/heapdump.hprof"
        )));
    }

    @Test
    void requiresHeapDumpOnOutOfMemoryError() {
        assertFalse(JvmDiagnostics.hasRecommendedHeapDumpOptions(List.of(
                "-XX:-HeapDumpOnOutOfMemoryError",
                "-XX:HeapDumpPath=/tmp/heapdump.hprof"
        )));
    }

    @Test
    void requiresHeapDumpPath() {
        assertFalse(JvmDiagnostics.hasRecommendedHeapDumpOptions(List.of(
                "-XX:+HeapDumpOnOutOfMemoryError"
        )));
    }

    @Test
    void recognizesOptionsEmbeddedInRuntimeArguments() {
        assertTrue(JvmDiagnostics.hasRecommendedHeapDumpOptions(List.of(
                "wrapper=-XX:+HeapDumpOnOutOfMemoryError",
                "wrapper=-XX:HeapDumpPath=/tmp/heapdump.hprof"
        )));
    }
}
