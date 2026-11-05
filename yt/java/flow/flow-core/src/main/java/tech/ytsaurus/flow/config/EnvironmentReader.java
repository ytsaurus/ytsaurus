package tech.ytsaurus.flow.config;

import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * System.getenv wrapper.
 * Provides: getVarOptional method and ability for mock environment variables resolving for unit tests.
 */
public class EnvironmentReader {
    /**
     * Flow environment variables names.
     */
    // YT_FLOW_CONFIG: contains patch to config file in yson format (patch itself, not path to file!).
    public static final String ENV_VAR_YT_FLOW_CONFIG = "YT_FLOW_CONFIG";
    // YT_FLOW_MODE: Controller, Worker, or empty.
    public static final String ENV_VAR_FLOW_MODE = "YT_FLOW_MODE";
    // YT_PROXY_URL_ALIASING_CONFIG: contains a yson map of YT cluster alias to cluster url.
    public static final String ENV_VAR_YT_PROXY_URL_ALIASING_CONFIG = "YT_PROXY_URL_ALIASING_CONFIG";
    // YT_FLOW_WAIT: flag indicating if runner must wait for pipeline completion (1: wait, 0: do not wait)
    public static final String ENV_VAR_YT_FLOW_WAIT = "YT_FLOW_WAIT";
    // YT_FLOW_GRACEFUL_UPDATE: flag indicating if runner must wait for pipeline stop before update
    // (1: wait, 0: do not wait)
    public static final String ENV_VAR_YT_FLOW_GRACEFUL_UPDATE = "YT_FLOW_GRACEFUL_UPDATE";

    public @Nullable String getVar(String name) {
        return System.getenv(name);
    }

    public String getVar(String name, String defaultValue) {
        var value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    public Optional<String> getVarOptional(String name) {
        return Optional.ofNullable(System.getenv(name));
    }
}
