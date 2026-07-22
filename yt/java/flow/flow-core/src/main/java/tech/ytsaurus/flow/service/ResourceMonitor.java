package tech.ytsaurus.flow.service;

import java.lang.management.ManagementFactory;
import java.time.Duration;

import com.sun.management.ThreadMXBean;
import tech.ytsaurus.core.DataSize;

public class ResourceMonitor {
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
     * Method calls provided callback and measures CPU time and allocated memory used in process of callback execution.
     * <p>
     * If JVM doesn't support CPU time measurement, {@code ResourceStats#cpuTime} would be set to zero.
     * <p>
     * If JVM doesn't support thread memory allocation measurement, {@code ResourceStats#allocatedBytes} would be set
     * to zero.
     * <p>
     * User code at provided Callback must be single-threaded and do not spawn other threads.
     *
     * @param action Callback to measure.
     * @return ResourceStats object with measured CPU time and allocated memory.
     * @throws Exception If action throws an exception.
     */
    public ResourceStats callMeasured(Callback action) throws Exception {
        // All THREAD_MX_BEAN calls wrapped with Math.max(0L, ...) because it may return -1 if required measurement
        // is not supported by JVM.
        long allocationsBeforeCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadAllocatedBytes());
        long cpuTimeBeforeCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadCpuTime());
        action.call();
        long cpuTimeAfterCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadCpuTime());
        long allocationsAfterCall = Math.max(0L, THREAD_MX_BEAN.getCurrentThreadAllocatedBytes());
        Duration callDuration = Duration.ofNanos(cpuTimeAfterCall - cpuTimeBeforeCall);
        DataSize allocationSize = DataSize.fromBytes(allocationsAfterCall - allocationsBeforeCall);
        return new ResourceStats(allocationSize, callDuration);
    }

}
