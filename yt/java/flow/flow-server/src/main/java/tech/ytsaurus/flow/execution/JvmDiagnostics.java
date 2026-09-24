package tech.ytsaurus.flow.execution;

import java.lang.management.ManagementFactory;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports recommended JVM settings when a companion process starts.
 */
final class JvmDiagnostics {
    private static final Logger log = LoggerFactory.getLogger(JvmDiagnostics.class);

    private JvmDiagnostics() {
    }

    static void warnIfHeapDumpOptionsNotSet() {
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        boolean onOom = hasArgument(arguments, "-XX:+HeapDumpOnOutOfMemoryError");
        boolean path = hasArgument(arguments, "-XX:HeapDumpPath");
        if (!hasRecommendedHeapDumpOptions(arguments)) {
            log.warn("Heap dump JVM options are not configured. "
                            + "It is recommended to set -XX:+HeapDumpOnOutOfMemoryError and "
                            + "-XX:HeapDumpPath=<path> via YT_FLOW_COMPANION_JVM_EXTRA_OPTS environment variable "
                            + "to enable heap dump generation on OutOfMemoryError "
                            + "(HasHeapDumpOnOutOfMemoryError: {}, HasHeapDumpPath: {}, JvmArgs: {})",
                    onOom, path, arguments);
        }
    }

    static boolean hasRecommendedHeapDumpOptions(List<String> arguments) {
        return hasArgument(arguments, "-XX:+HeapDumpOnOutOfMemoryError")
                && hasArgument(arguments, "-XX:HeapDumpPath");
    }

    private static boolean hasArgument(List<String> arguments, String option) {
        return arguments.stream().anyMatch(argument -> argument.contains(option));
    }
}
