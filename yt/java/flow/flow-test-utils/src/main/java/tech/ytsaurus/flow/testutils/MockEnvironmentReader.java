package tech.ytsaurus.flow.testutils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.config.EnvironmentReader;

/**
 * In-memory test double for {@link EnvironmentReader}.
 */
public class MockEnvironmentReader extends EnvironmentReader {
    private Map<String, String> env = new HashMap<>();

    public MockEnvironmentReader() {
    }

    public MockEnvironmentReader(Map<String, String> env) {
        this.env = env;
    }

    public MockEnvironmentReader worker() {
        this.env.put(ENV_VAR_FLOW_MODE, "Worker");
        return this;
    }

    public MockEnvironmentReader companionConfig(CompanionExecutionConfig config) {
        this.env.put(CompanionExecutionConfig.ENV_VAR_COMPANION_CONFIG, config.toYTreeText());
        return this;
    }

    public void setVar(String name, String value) {
        env.put(name, value);
    }

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
        return Optional.ofNullable(getVar(name));
    }
}
