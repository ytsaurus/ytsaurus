package tech.ytsaurus.flow.pipeline;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * Job environment that mounts no layers: the JDK comes from outside the launcher — the task's
 * docker image on CRI clusters, or the host JDK under the {@code YT_FLOW_JDK_LAYERS=[]} override
 * of the local integration tests. The companion java binary must be set explicitly.
 */
final class DockerJobEnvironment extends AbstractJobEnvironment {

    DockerJobEnvironment(EnvironmentReader envReader) {
        super(envReader);
    }

    @Override
    protected void patchTaskConfig(YTreeMapNode task) {
        // The hand-written task survives verbatim: the job environment supplies the JDK.
    }

    @Override
    protected String doResolveJdkBinPath(@Nullable String handWrittenBinPath) {
        if (handWrittenBinPath != null && !handWrittenBinPath.isBlank()) {
            return handWrittenBinPath;
        }
        // Images place java differently and the worker spawns it by the exact path, so demand it
        // up front instead of failing at job runtime.
        throw new IllegalStateException(
                "No JDK layer is mounted, so the java binary of the job environment must be set"
                        + " explicitly: put jdk_bin_path into the companion resource parameters"
                        + " or set " + ENV_VAR_JDK_BIN_PATH + ", e.g. /opt/java/openjdk/bin/java");
    }
}
