package tech.ytsaurus.flow.spring;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlowEnvironmentPostProcessor}.
 */
class FlowEnvironmentPostProcessorTest {

    private final FlowEnvironmentPostProcessor processor = new FlowEnvironmentPostProcessor();

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
}
