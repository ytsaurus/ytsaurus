package tech.ytsaurus.flow.service;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.Callable;

import com.sun.management.ThreadMXBean;
import tech.ytsaurus.core.DataSize;

public class ExecutionMeter {
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    static {
        if (THREAD_MX_BEAN.isThreadCpuTimeSupported()) {
            THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
        }
        if (THREAD_MX_BEAN.isThreadAllocatedMemorySupported()) {
            THREAD_MX_BEAN.setThreadAllocatedMemoryEnabled(true);
        }
    }

    /**
     * Measures CPU time and allocations on the calling thread and returns the callback result.
     * Work on other threads is not included; callback failures propagate unchanged.
     */
    public <T> Measured<T> measure(Callable<T> action) throws Exception {
        // All THREAD_MX_BEAN calls wrapped with Math.max(0L, ...) because it may return -1 if required measurement
        // is not supported by JVM.
        long allocationsBeforeCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadAllocatedBytes());
        long cpuTimeBeforeCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadCpuTime());
        T result = action.call();
        long cpuTimeAfterCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadCpuTime());
        long allocationsAfterCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadAllocatedBytes());
        Duration callDuration = Duration.ofNanos(cpuTimeAfterCall - cpuTimeBeforeCall);
        DataSize allocationSize = DataSize.fromBytes(allocationsAfterCall - allocationsBeforeCall);
        return new Measured<>(result, new ResourceStats(allocationSize, callDuration));
    }

    /**
     * A callback result together with its calling-thread resource consumption.
     */
    public record Measured<T>(T value, ResourceStats stats) {
    }
}
