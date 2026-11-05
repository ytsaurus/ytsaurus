package tech.ytsaurus.flow.spring;

import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.springframework.core.env.Environment;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.FlowRunMode;

/**
 * Resolves the Flow run mode a Spring application runs in.
 *
 * <p>The mode comes from {@code YT_FLOW_MODE}, which the worker exports to the companion it spawns.
 * The {@code flow.run-mode} property applies only when the variable is unset, which is how a test
 * selects a mode its JVM cannot express.
 *
 * @see FlowProperties#getRunMode()
 */
final class FlowRunModeResolver {

    static final String RUN_MODE_PROPERTY = FlowProperties.RUN_MODE_PROPERTY;

    /**
     * The value of {@code flow.run-mode} that selects the runner explicitly, mirroring an unset
     * {@code YT_FLOW_MODE}.
     */
    static final String RUNNER_MODE = FlowProperties.RUNNER_MODE;

    private FlowRunModeResolver() {
    }

    /**
     * @param environment the Spring environment carrying the {@code flow.run-mode} override.
     * @return the declared mode, or an empty optional in runner mode.
     * @throws IllegalArgumentException if either source holds an unknown mode name.
     * @throws IllegalStateException    if the property contradicts a set {@code YT_FLOW_MODE}, or
     *                                  either source selects Controller.
     */
    static Optional<FlowRunMode> resolve(Environment environment) {
        return resolve(environment, new EnvironmentReader());
    }

    static Optional<FlowRunMode> resolve(Environment environment, EnvironmentReader envReader) {
        Optional<FlowRunMode> environmentMode =
                FlowRunMode.fromEnvironment(envReader).map(FlowRunModeResolver::rejectController);
        Optional<FlowRunMode> propertyMode = resolveProperty(environment);

        if (environmentMode.isPresent()) {
            // The environment wins, and a contradicting property is loud rather than ignored: a
            // stale one must not re-role a worker-spawned companion.
            if (propertyMode != null && !propertyMode.equals(environmentMode)) {
                throw new IllegalStateException(
                        "%s=%s contradicts %s=%s; remove the property outside tests".formatted(
                                RUN_MODE_PROPERTY, environment.getProperty(RUN_MODE_PROPERTY),
                                EnvironmentReader.ENV_VAR_FLOW_MODE, environmentMode.get()));
            }
            return environmentMode;
        }
        return propertyMode != null ? propertyMode : Optional.empty();
    }

    /**
     * The mode the {@code flow.run-mode} property selects: an empty optional for {@code runner},
     * a mode for a mode name (case-insensitive), {@code null} when the property is not set.
     */
    private static @Nullable Optional<FlowRunMode> resolveProperty(Environment environment) {
        String property = environment.getProperty(RUN_MODE_PROPERTY);
        if (property == null || property.isBlank()) {
            return null;
        }
        String value = property.trim();
        if (RUNNER_MODE.equalsIgnoreCase(value)) {
            return Optional.empty();
        }
        for (FlowRunMode mode : FlowRunMode.values()) {
            if (mode.name().equalsIgnoreCase(value)) {
                return Optional.of(rejectController(mode));
            }
        }
        throw new IllegalArgumentException(
                "%s has unknown value \"%s\"; expected Worker, Controller or %s (case-insensitive)".formatted(
                        RUN_MODE_PROPERTY, property, RUNNER_MODE));
    }

    /**
     * Controller mode has no role here — no launch, no companion — so it fails startup rather than
     * idling forever under keep-alive.
     */
    static FlowRunMode rejectController(FlowRunMode mode) {
        if (mode == FlowRunMode.Controller) {
            throw new IllegalStateException("Controller mode is not supported yet, got %s".formatted(mode));
        }
        return mode;
    }

    /**
     * @param environment the Spring environment, consulted before the process environment.
     * @return whether the application launches the pipeline rather than serving it.
     */
    static boolean isRunnerMode(Environment environment) {
        return resolve(environment).isEmpty();
    }
}
