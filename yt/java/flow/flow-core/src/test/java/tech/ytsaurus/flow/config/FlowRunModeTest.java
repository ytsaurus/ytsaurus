package tech.ytsaurus.flow.config;

import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowRunModeTest {

    @Test
    public void testAbsentModeIsRunner() {
        var envReader = envWith(Map.of());

        assertEquals(Optional.empty(), FlowRunMode.fromEnvironment(envReader));
    }

    @Test
    public void testBlankModeIsRunner() {
        var envReader = envWith(Map.of(EnvironmentReader.ENV_VAR_FLOW_MODE, "   "));

        assertEquals(Optional.empty(), FlowRunMode.fromEnvironment(envReader));
    }

    @Test
    public void testWorkerMode() {
        var envReader = envWith(Map.of(EnvironmentReader.ENV_VAR_FLOW_MODE, "Worker"));

        assertEquals(Optional.of(FlowRunMode.Worker), FlowRunMode.fromEnvironment(envReader));
    }

    @Test
    public void testControllerMode() {
        var envReader = envWith(Map.of(EnvironmentReader.ENV_VAR_FLOW_MODE, "Controller"));

        assertEquals(Optional.of(FlowRunMode.Controller), FlowRunMode.fromEnvironment(envReader));
    }

    @Test
    public void testUnknownModeThrowsAnActionableError() {
        var envReader = envWith(Map.of(EnvironmentReader.ENV_VAR_FLOW_MODE, "worker"));

        var error = assertThrows(IllegalArgumentException.class, () -> FlowRunMode.fromEnvironment(envReader));
        // The message must name the variable and the accepted values.
        assertTrue(error.getMessage().contains(EnvironmentReader.ENV_VAR_FLOW_MODE), error.getMessage());
        assertTrue(error.getMessage().contains("Worker"), error.getMessage());
    }

    private static EnvironmentReader envWith(Map<String, String> env) {
        return new EnvironmentReader() {
            @Override
            public @Nullable String getVar(String name) {
                return env.get(name);
            }

            @Override
            public String getVar(String name, String defaultValue) {
                return env.getOrDefault(name, defaultValue);
            }

            @Override
            public Optional<String> getVarOptional(String name) {
                return Optional.ofNullable(env.get(name));
            }
        };
    }
}
