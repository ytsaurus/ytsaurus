package tech.ytsaurus.flow.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;


class ResourceMonitorTest {

    @Test
    public void testStats() throws Exception {
        var resourceMonitor = new ResourceMonitor();
        var stats = resourceMonitor.callMeasured(() -> {
            long length = 0;
            for (int i = 0; i < 1000; ++i) {
                var bytes = new byte[1000];
                length += bytes.length;
            }
            // In order to make 'length' variable used.
            System.out.println(length);
        });
        // 1000 * 1000 + some fixed costs.
        assertTrue(stats.getAllocatedBytes().toBytes() > 1_000_000);
        // Fixed costs shouldn't be so huge.
        assertTrue(stats.getAllocatedBytes().toBytes() < 1_100_000);
        // CPU time should be greater than 0ns.
        assertTrue(stats.getCpuTime().toNanos() > 0);
    }

}
