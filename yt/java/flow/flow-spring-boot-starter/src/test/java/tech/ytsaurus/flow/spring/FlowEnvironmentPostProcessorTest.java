package tech.ytsaurus.flow.spring;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.testutils.MockEnvironmentReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlowEnvironmentPostProcessor}.
 */
class FlowEnvironmentPostProcessorTest {

    // YT_FLOW_MODE cannot be set in the JVM of a test, so the reader is injected instead. The
    // post-processor deliberately ignores the flow.run-mode property.
    private final FlowEnvironmentPostProcessor processor = processorFor("Worker");
    private final FlowEnvironmentPostProcessor runnerProcessor = processorFor(null);

    @Test
    void setsKeepAliveWhenNotPresent() {
        ConfigurableEnvironment environment = new StandardEnvironment();
        processor.postProcessEnvironment(environment, new SpringApplication());

        assertTrue(environment.containsProperty("spring.main.keep-alive"));
        assertEquals("true", environment.getProperty("spring.main.keep-alive"));
    }

    @Test
    void doesNotOverrideExistingKeepAliveProperty() {
        ConfigurableEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(
                new MapPropertySource("test", Map.of("spring.main.keep-alive", "false"))
        );

        processor.postProcessEnvironment(environment, new SpringApplication());

        assertEquals("false", environment.getProperty("spring.main.keep-alive"),
                "User-defined property should not be overridden");
    }

    @Test
    void propertyHasLowestPriority() {
        ConfigurableEnvironment environment = new StandardEnvironment();
        processor.postProcessEnvironment(environment, new SpringApplication());

        // Add a user property source after the processor has run
        environment.getPropertySources().addFirst(
                new MapPropertySource("userConfig", Map.of("spring.main.keep-alive", "false"))
        );

        assertEquals("false", environment.getProperty("spring.main.keep-alive"),
                "User-defined property should take precedence over flow defaults");
    }

    @Test
    void runnerModeDoesNotKeepTheJvmAliveAndNeedsNoServer() {
        ConfigurableEnvironment environment = new StandardEnvironment();

        runnerProcessor.postProcessEnvironment(environment, new SpringApplication());

        assertEquals("false", environment.getProperty("spring.main.keep-alive"));
        assertEquals("none", environment.getProperty("spring.main.web-application-type"));
        assertEquals("true", environment.getProperty("spring.main.lazy-initialization"));
    }

    @Test
    void workerModeDoesNotEnableLazyInitialization() {
        ConfigurableEnvironment environment = new StandardEnvironment();

        processor.postProcessEnvironment(environment, new SpringApplication());

        // FlowCompanionLifecycle must be created eagerly.
        assertNull(environment.getProperty("spring.main.lazy-initialization"));
    }

    @Test
    void runnerModeDefaultsAreOverridable() {
        ConfigurableEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource(
                "test", Map.of("spring.main.lazy-initialization", "false")));

        runnerProcessor.postProcessEnvironment(environment, new SpringApplication());

        assertEquals("false", environment.getProperty("spring.main.lazy-initialization"));
    }

    @Test
    void runModePropertyDoesNotDriveTheDefaults() {
        // The property reaches the environment too late to be trusted here, so a worker JVM keeps
        // its companion defaults even when the property says otherwise, and vice versa.
        ConfigurableEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource(
                "test", Map.of(FlowProperties.RUN_MODE_PROPERTY, FlowProperties.RUNNER_MODE)));

        processor.postProcessEnvironment(environment, new SpringApplication());

        assertEquals("true", environment.getProperty("spring.main.keep-alive"));
        assertNull(environment.getProperty("spring.main.lazy-initialization"));
    }

    @Test
    void controllerModeFailsTheEnvironmentPreparation() {
        ConfigurableEnvironment environment = new StandardEnvironment();

        assertThrows(
                IllegalStateException.class,
                () -> processorFor("Controller").postProcessEnvironment(environment, new SpringApplication()));
    }

    @Test
    void testCreatedContextGetsNoDefaultsAtAll() {
        // This call runs under JUnit, so the real, unoverridden detection must recognize the stack:
        // a test context keeps vanilla Spring semantics — no keep-alive, no web-application-type,
        // no lazy-initialization.
        ConfigurableEnvironment environment = new StandardEnvironment();
        var envReader = new MockEnvironmentReader();
        new FlowEnvironmentPostProcessor(envReader).postProcessEnvironment(environment, new SpringApplication());

        assertNull(environment.getProperty("spring.main.keep-alive"));
        assertNull(environment.getProperty("spring.main.web-application-type"));
        assertNull(environment.getProperty("spring.main.lazy-initialization"));
    }

    /**
     * A processor with the test detection forced off, so the production defaults are observable
     * from a JUnit-driven test.
     */
    private static FlowEnvironmentPostProcessor processorFor(@Nullable String flowMode) {
        var envReader = new MockEnvironmentReader();
        if (flowMode != null) {
            envReader.setVar(EnvironmentReader.ENV_VAR_FLOW_MODE, flowMode);
        }
        return new FlowEnvironmentPostProcessor(envReader) {
            @Override
            boolean startedByTestFramework() {
                return false;
            }
        };
    }
}
