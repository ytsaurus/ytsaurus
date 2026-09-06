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
    // Path to the java binary inside the job environment; wins over the hand-written resource
    // jdk_bin_path and the JDK layer's default binary.
    String ENV_VAR_JDK_BIN_PATH = "YT_FLOW_JDK_BIN_PATH";

    /** Patches the vanilla task configs with everything this environment needs at job runtime. */
    void patchVanillaConfig(YTreeMapNode vanilla);

    /**
     * Resolves the java binary the worker spawns the companion with; |handWrittenBinPath| is the
     * {@code jdk_bin_path} written in the companion resource parameters, if any.
     */
    String resolveJdkBinPath(@Nullable String handWrittenBinPath);
}
