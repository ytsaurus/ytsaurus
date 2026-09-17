package tech.ytsaurus.flow.service;

/**
 * Compatibility adapter for callers measuring a void callback. New code can use ExecutionMeter.
 */
public class ResourceMonitor extends ExecutionMeter {
    public ResourceStats callMeasured(Callback action) throws Exception {
        return measure(() -> {
            action.call();
            return true;
        }).stats();
    }
}
