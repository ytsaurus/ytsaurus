package tech.ytsaurus.flow.config;

import java.util.Optional;

/**
 * YT Flow Run mode: Worker or Controller.
 * C++ equivalent is the NYT::NFlow::EFlowRunMode.
 * <p>
 * An absent {@code YT_FLOW_MODE} means the process runs as the pipeline runner, so
 * {@link #fromEnvironment(EnvironmentReader)} returns an empty optional rather than a mode.
 */
public enum FlowRunMode {
    Worker(1),
    Controller(2);

    private final int order;

    FlowRunMode(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    /**
     * Resolves the run mode from {@link EnvironmentReader#ENV_VAR_FLOW_MODE}.
     *
     * @param envReader the environment to read the mode from.
     * @return the declared mode, or an empty optional when the variable is absent or blank
     * (the runner).
     * @throws IllegalArgumentException if the variable holds an unknown mode name.
     */
    public static Optional<FlowRunMode> fromEnvironment(EnvironmentReader envReader) {
        return envReader.getVarOptional(EnvironmentReader.ENV_VAR_FLOW_MODE)
                .map(String::trim)
                .filter(raw -> !raw.isEmpty())
                .map(FlowRunMode::parse);
    }

    private static FlowRunMode parse(String raw) {
        try {
            return valueOf(raw);
        } catch (IllegalArgumentException e) {
            // Name the variable and the accepted values: this ends up in a crash log.
            throw new IllegalArgumentException(
                    ("%s has unknown value \"%s\"; expected %s or %s (case-sensitive), or unset for "
                            + "the runner").formatted(
                            EnvironmentReader.ENV_VAR_FLOW_MODE, raw, Worker, Controller));
        }
    }

}
