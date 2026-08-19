package tech.ytsaurus.flow.spring;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.FlowRunMode;

/**
 * Supplies the Spring defaults each Flow run mode needs.
 * <p>
 * Companion mode ({@code YT_FLOW_MODE=Worker}) keeps the JVM alive, since the gRPC server runs on
 * daemon threads. Runner mode ({@code YT_FLOW_MODE} unset) submits the spec and exits, so it needs
 * no server and no eager beans: keep-alive off, web-application-type none, lazy initialization on.
 * <p>
 * A test context gets no defaults at all — a test keeps vanilla Spring semantics. Defaults are added
 * with the lowest priority, so an application can override any of them.
 * <p>
 * The mode is read from {@code YT_FLOW_MODE} alone: the {@code flow.run-mode} property may not be
 * part of the environment yet at this point, and honouring it only sometimes would be worse.
 */
public class FlowEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static final String PROPERTY_SOURCE_NAME = "flowDefaults";
    private static final String KEEP_ALIVE_PROPERTY = "spring.main.keep-alive";
    private static final String WEB_APPLICATION_TYPE_PROPERTY = "spring.main.web-application-type";
    private static final String LAZY_INITIALIZATION_PROPERTY = "spring.main.lazy-initialization";

    private final EnvironmentReader envReader;

    public FlowEnvironmentPostProcessor() {
        this(new EnvironmentReader());
    }

    FlowEnvironmentPostProcessor(EnvironmentReader envReader) {
        this.envReader = envReader;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (startedByTestFramework()) {
            // Forcing a non-web context or lazy beans would change what user tests observe.
            return;
        }

        Map<String, Object> defaults = new LinkedHashMap<>();
        // The environment variable only: a property set by the application may not be visible yet
        // here, and would then contradict the mode the beans are selected with.
        // Controller mode has no role here, so fail startup rather than idle under keep-alive.
        Optional<FlowRunMode> runMode = FlowRunMode.fromEnvironment(envReader);
        runMode.ifPresent(FlowRunModeResolver::rejectController);
        if (runMode.isEmpty()) {
            putIfAbsent(environment, defaults, KEEP_ALIVE_PROPERTY, "false");
            putIfAbsent(environment, defaults, WEB_APPLICATION_TYPE_PROPERTY, "none");
            putIfAbsent(environment, defaults, LAZY_INITIALIZATION_PROPERTY, "true");
        } else {
            putIfAbsent(environment, defaults, KEEP_ALIVE_PROPERTY, "true");
        }

        if (!defaults.isEmpty()) {
            environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, defaults));
        }
    }

    /** Overridable so a test can force the production branch. */
    boolean startedByTestFramework() {
        return TestFrameworkDetector.testFrameworkOnStack();
    }

    private static void putIfAbsent(
            ConfigurableEnvironment environment,
            Map<String, Object> defaults,
            String property,
            String value
    ) {
        if (!environment.containsProperty(property)) {
            defaults.put(property, value);
        }
    }
}
