package tech.ytsaurus.flow.pipeline;

import java.nio.file.Path;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.ysontree.YTreeMapNode;

/**
 * Job environment that mounts no layers: the JDK comes from outside the launcher — the task's
 * docker image on CRI clusters, or the host JDK under the {@code YT_FLOW_JDK_LAYERS=[]} override
 * of the local integration tests.
 */
final class DockerJobEnvironment extends AbstractJobEnvironment {
    private static final Logger log = LoggerFactory.getLogger(DockerJobEnvironment.class);

    // The java.home of the launcher JVM: the launcher runs in the same environment as the jobs
    // (the flow-java image, or the host of the local tests), so its java is the java of the job.
    private final String launcherJavaHome;

    DockerJobEnvironment(EnvironmentReader envReader, String launcherJavaHome) {
        super(envReader);
        this.launcherJavaHome = launcherJavaHome;
    }

    @Override
    protected void patchTaskConfig(YTreeMapNode task) {
        // The hand-written task survives verbatim: the job environment supplies the JDK.
    }

    @Override
    protected String doResolveJdkBinPath(@Nullable String handWrittenBinPath, @Nullable String envBinPath) {
        if (handWrittenBinPath != null) {
            return handWrittenBinPath;
        }
        if (envBinPath != null) {
            return envBinPath;
        }
        String binPath = Path.of(launcherJavaHome, "bin", "java").toString();
        log.info("No jdk_bin_path is set, taking the java the launcher runs on: {}", binPath);
        return binPath;
    }
}
