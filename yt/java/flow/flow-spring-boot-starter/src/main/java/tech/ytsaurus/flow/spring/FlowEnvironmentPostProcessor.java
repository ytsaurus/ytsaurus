package tech.ytsaurus.flow.spring;

import java.util.Map;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * An {@link EnvironmentPostProcessor} that sets {@code spring.main.keep-alive=true}
 * as a default property with the lowest priority.
 * <p>
 * This ensures the JVM stays alive after the gRPC companion server is started
 * asynchronously via {@link FlowCompanionLifecycle}, since gRPC's internal Netty
 * threads are daemon threads and would otherwise allow the JVM to exit.
 * <p>
 * The property is added with the lowest priority, so users can override it by setting
 * {@code spring.main.keep-alive=false} in their own configuration if needed.
 *
 * @see FlowCompanionLifecycle
 * @see FlowAutoConfiguration
 */
public class FlowEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static final String PROPERTY_SOURCE_NAME = "flowDefaults";
    private static final String KEEP_ALIVE_PROPERTY = "spring.main.keep-alive";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (!environment.containsProperty(KEEP_ALIVE_PROPERTY)) {
            environment.getPropertySources().addLast(
                    new MapPropertySource(PROPERTY_SOURCE_NAME, Map.of(KEEP_ALIVE_PROPERTY, "true"))
            );
        }
    }
}
