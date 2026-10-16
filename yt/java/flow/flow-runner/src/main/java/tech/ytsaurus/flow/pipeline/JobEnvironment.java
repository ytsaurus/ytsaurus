package tech.ytsaurus.flow.pipeline;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * The job environment of the vanilla tasks: how a JDK reaches the job and which java binary the
 * worker spawns the companion with.
 *
 * <p>{@link JobEnvironmentResolver} picks the implementation from the hand-written vanilla config:
 * a {@code docker_image} on any task selects {@link DockerJobEnvironment}, otherwise
 * {@link PortoJobEnvironment} mounts the built-in JDK porto layer.
 */
interface JobEnvironment {
    // Path to the java binary inside the job environment: the fallback for a companion resource
    // without jdk_bin_path and the override of a launcher-mounted JDK layer's binary.
    String ENV_VAR_JDK_BIN_PATH = "YT_FLOW_JDK_BIN_PATH";

    /** Patches the vanilla task configs with everything this environment needs at job runtime. */
    void patchVanillaConfig(YTreeMapNode vanilla);

    /**
     * Resolves the java binary the worker spawns the companion with; |handWrittenBinPath| is the
     * {@code jdk_bin_path} written in the companion resource parameters, if any. The hand-written
     * path comes first, then {@code YT_FLOW_JDK_BIN_PATH}, then the environment's own default;
     * a launcher-mounted JDK layer owns the path and ignores a hand-written one.
     */
    String resolveJdkBinPath(@Nullable String handWrittenBinPath);
}
