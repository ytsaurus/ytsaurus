package tech.ytsaurus.flow.spring;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.FlowRunMode;
import tech.ytsaurus.flow.testutils.MockEnvironmentReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlowRunModeResolver}: both sources, their precedence and their failures. */
class FlowRunModeResolverTest {

    @Test
    void environmentVariableSelectsTheMode() {
        var resolved = FlowRunModeResolver.resolve(environmentWith(null), new MockEnvironmentReader().worker());

        assertEquals(Optional.of(FlowRunMode.Worker), resolved);
    }

    @Test
    void blankEnvironmentVariableMeansRunner() {
        var envReader = new MockEnvironmentReader();
        envReader.setVar(EnvironmentReader.ENV_VAR_FLOW_MODE, "   ");

        assertEquals(Optional.empty(), FlowRunModeResolver.resolve(environmentWith(null), envReader));
    }

    @Test
    void environmentControllerIsRejected() {
        var envReader = new MockEnvironmentReader();
        envReader.setVar(EnvironmentReader.ENV_VAR_FLOW_MODE, "Controller");

        assertThrows(
                IllegalStateException.class,
                () -> FlowRunModeResolver.resolve(environmentWith(null), envReader));
    }

    @Test
    void propertyIsCaseInsensitive() {
        // Mode names must not behave differently from "runner".
        assertEquals(
                Optional.of(FlowRunMode.Worker),
                FlowRunModeResolver.resolve(environmentWith("worker"), new MockEnvironmentReader()));
        assertEquals(
                Optional.empty(),
                FlowRunModeResolver.resolve(environmentWith("RUNNER"), new MockEnvironmentReader()));
    }

    @Test
    void unknownPropertyValueNamesThePropertyAndTheChoices() {
        var error = assertThrows(
                IllegalArgumentException.class,
                () -> FlowRunModeResolver.resolve(environmentWith("wroker"), new MockEnvironmentReader()));
        assertTrue(error.getMessage().contains(FlowProperties.RUN_MODE_PROPERTY), error.getMessage());
        assertTrue(error.getMessage().contains("Worker"), error.getMessage());
    }

    @Test
    void theRealEnvironmentWinsAndADisagreementIsLoud() {
        // A stale property must neither re-role the process nor be silently ignored.
        var error = assertThrows(
                IllegalStateException.class,
                () -> FlowRunModeResolver.resolve(
                        environmentWith("runner"), new MockEnvironmentReader().worker()));
        assertTrue(error.getMessage().contains(FlowProperties.RUN_MODE_PROPERTY), error.getMessage());
        assertTrue(error.getMessage().contains(EnvironmentReader.ENV_VAR_FLOW_MODE), error.getMessage());
    }

    @Test
    void agreementBetweenPropertyAndEnvironmentIsFine() {
        assertEquals(
                Optional.of(FlowRunMode.Worker),
                FlowRunModeResolver.resolve(environmentWith("Worker"), new MockEnvironmentReader().worker()));
    }

    private static StandardEnvironment environmentWith(String runModeProperty) {
        var environment = new StandardEnvironment();
        if (runModeProperty != null) {
            environment.getPropertySources().addFirst(new MapPropertySource(
                    "test", Map.of(FlowProperties.RUN_MODE_PROPERTY, runModeProperty)));
        }
        return environment;
    }
}
