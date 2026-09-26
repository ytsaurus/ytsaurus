package tech.ytsaurus.flow.pipeline;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Base of the job environments: iterates the vanilla tasks and normalizes the hand-written
 * {@code jdk_bin_path} and the {@code YT_FLOW_JDK_BIN_PATH} value for the environment-specific
 * resolution.
 */
abstract class AbstractJobEnvironment implements JobEnvironment {
    protected final EnvironmentReader envReader;

    protected AbstractJobEnvironment(EnvironmentReader envReader) {
        this.envReader = envReader;
    }

    @Override
    public final void patchVanillaConfig(YTreeMapNode vanilla) {
        patchDeclaredTask(vanilla, "worker");
        // An absent controller section is preserved: flow_server defaults it, while a map
        // created here would lack the required "count".
        patchDeclaredTask(vanilla, "controller");
    }

    @Override
    public final String resolveJdkBinPath(@Nullable String handWrittenBinPath) {
        String envBinPath = envReader.getVarOptional(ENV_VAR_JDK_BIN_PATH)
                .filter(path -> !path.isBlank())
                .orElse(null);
        return doResolveJdkBinPath(nonBlankOrNull(handWrittenBinPath), envBinPath);
    }

    /** Patches one vanilla task config for this environment. */
    protected abstract void patchTaskConfig(YTreeMapNode task);

    /**
     * Resolves the java binary from the non-blank hand-written path and env value, each null
     * when not set; see #resolveJdkBinPath.
     */
    protected abstract String doResolveJdkBinPath(@Nullable String handWrittenBinPath, @Nullable String envBinPath);

    private static @Nullable String nonBlankOrNull(@Nullable String path) {
        return path == null || path.isBlank() ? null : path;
    }

    private void patchDeclaredTask(YTreeMapNode vanilla, String taskKey) {
        vanilla.get(taskKey)
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::mapNode)
                .ifPresent(this::patchTaskConfig);
    }
}
