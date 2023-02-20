package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.operations.JarsProcessor;
import tech.ytsaurus.client.operations.SingleUploadFromClassPathJarsProcessor;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.JavaOptions;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;


@NonNullApi
@NonNullFields
public class YtClientConfiguration {
    private final RpcOptions rpcOptions;
    @Nullable
    private final YTreeNode specPatch;
    @Nullable
    private final YTreeNode jobSpecPatch;

    private final YPath tmpDir;
    @Nullable
    private final YPath cacheDir;
    private final JarsProcessor jarsProcessor;
    private final boolean isLocalMode;
    private final String javaBinary;
    private final JavaOptions javaOptions;
    private final Duration jarsUploadTimeout;
    private final int fileCacheReplicationFactor;
    private final String version;
    private final Duration operationPingPeriod;

    YtClientConfiguration(Builder builder) {
        if (builder.rpcOptions == null) {
            throw new IllegalStateException("Expected not null rpcOptions");
        }
        this.rpcOptions = builder.rpcOptions;

        if (builder.jarsProcessor == null) {
            throw new IllegalStateException("jarsProcessor is null");
        }
        this.jarsProcessor = builder.jarsProcessor;

        this.tmpDir = builder.tmpDir;
        this.cacheDir = builder.cacheDir;
        this.isLocalMode = builder.isLocalMode;
        this.javaBinary = builder.javaBinary;
        this.javaOptions = builder.javaOptions;
        this.jarsUploadTimeout = builder.jarsUploadTimeout;
        this.fileCacheReplicationFactor = builder.fileCacheReplicationFactor;
        this.version = builder.version;
        this.specPatch = builder.specPatch;
        this.jobSpecPatch = builder.jobSpecPatch;
        this.operationPingPeriod = builder.operationPingPeriod;
    }

    public static Builder builder() {
        return new Builder().withPorto();
    }

    public RpcOptions getRpcOptions() {
        return rpcOptions;
    }

    public Optional<YTreeNode> getJobSpecPatch() {
        return Optional.ofNullable(jobSpecPatch);
    }

    public Optional<YTreeNode> getSpecPatch() {
        return Optional.ofNullable(specPatch);
    }

    public YPath getTmpDir() {
        return tmpDir;
    }

    public JarsProcessor getJarsProcessor() {
        return jarsProcessor;
    }

    public boolean isLocalMode() {
        return isLocalMode;
    }

    public String getJavaBinary() {
        return javaBinary;
    }

    public List<String> getJavaOptions() {
        return javaOptions.getOptions();
    }

    public String getVersion() {
        return version;
    }

    public Duration getOperationPingPeriod() {
        return operationPingPeriod;
    }

    private static int getJavaMajorVersion() {
        String javaVersion = System.getProperty("java.version");
        if (javaVersion != null) {
            try {
                String[] versionParts = javaVersion.split("\\.");
                if (versionParts.length == 1) {
                    // 11+ format
                    return Integer.parseInt(versionParts[0]);
                } else if (versionParts.length > 1) {
                    String p1 = versionParts[0];
                    String p2 = versionParts[1];
                    if (p1.equals("1")) {
                        // 1.8.x format
                        return Integer.parseInt(p2);
                    } else {
                        // 10.2.x format
                        return Integer.parseInt(p1);
                    }
                }
            } catch (NumberFormatException nfe) {
                return 0;
            }
        }
        // we don't want to raise exception on invalid local Java configuration
        return 0;
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        @Nullable
        private RpcOptions rpcOptions;
        private YPath tmpDir = YPath.simple("//tmp/yt_wrapper/file_storage");
        @Nullable
        private YPath cacheDir = YPath.simple("//tmp/yt_wrapper/file_storage").child("new_cache");

        @Nullable
        private JarsProcessor jarsProcessor;
        private boolean isLocalMode = false;
        private String javaBinary = "/usr/lib/jvm/java-8-oracle/bin/java";
        private JavaOptions javaOptions = JavaOptions.empty();
        private Duration jarsUploadTimeout = Duration.ofMinutes(10);
        private int fileCacheReplicationFactor = 10;
        private String version = "YTsaurusClient@";
        private Duration operationPingPeriod = Duration.ofSeconds(30);

        @Nullable
        private YTreeNode jobSpecPatch;
        @Nullable
        private YTreeNode specPatch;

        public Builder setRpcOptions(RpcOptions rpcOptions) {
            this.rpcOptions = rpcOptions;
            return this;
        }

        public Builder setTmpDir(YPath tmpDir) {
            this.tmpDir = tmpDir;
            return this;
        }

        public Builder setJarsProcessor(JarsProcessor jarsProcessor) {
            this.jarsProcessor = jarsProcessor;
            return this;
        }

        public Builder setIsLocalMode(boolean isLocalMode) {
            this.isLocalMode = isLocalMode;
            return this;
        }

        public Builder setJavaBinary(String javaBinary) {
            this.javaBinary = javaBinary;
            return this;
        }

        public Builder setJavaOptions(JavaOptions javaOptions) {
            this.javaOptions = javaOptions;
            return this;
        }

        public Builder addJavaOption(String javaOption) {
            this.javaOptions.withOption(javaOption);
            return this;
        }

        public Builder setCacheDir(@Nullable YPath cacheDir) {
            this.cacheDir = cacheDir;
            return this;
        }

        public Builder setJarsUploadTimeout(Duration jarsUploadTimeout) {
            this.jarsUploadTimeout = jarsUploadTimeout;
            return this;
        }

        public Builder setFileCacheReplicationFactor(int fileCacheReplicationFactor) {
            this.fileCacheReplicationFactor = fileCacheReplicationFactor;
            return this;
        }

        public Builder setVersion(String version) {
            this.version = version;
            return this;
        }

        public Builder setOperationPingPeriod(Duration operationPingPeriod) {
            this.operationPingPeriod = operationPingPeriod;
            return this;
        }

        public Builder setJobSpecPatch(@Nullable YTreeNode jobSpecPatch) {
            this.jobSpecPatch = jobSpecPatch;
            return this;
        }

        public Builder setSpecPatch(@Nullable YTreeNode specPatch) {
            this.specPatch = specPatch;
            return this;
        }

        public Builder withPorto() {
            javaOptions = JavaOptions.empty().withOption("-XX:+UseParallelGC");

            int javaMajorVersion = getJavaMajorVersion();
            if (javaMajorVersion >= 17) {
                javaBinary = "/opt/jdk17/bin/java";
            } else if (javaMajorVersion >= 15) {
                javaBinary = "/opt/jdk15/bin/java";
            } else if (javaMajorVersion >= 11) {
                javaBinary = "/opt/jdk11/bin/java";
            }

            this.jobSpecPatch = YTree.builder()
                    .beginMap()
                    .key("layer_paths").value(Arrays.asList(
                            "//porto_layers/delta/jdk/layer_with_jdk_lastest.tar.gz",
                            "//porto_layers/base/precise/" +
                                    "porto_layer_search_ubuntu_precise_app_lastest.tar.gz"
                    ))
                    .endMap()
                    .build();

            this.specPatch = YTree.builder()
                    .beginMap()
                    .key("scheduling_tag_filter").value("porto")
                    .endMap()
                    .build();

            return this;
        }

        public YtClientConfiguration build() {
            if (rpcOptions == null) {
                rpcOptions = new RpcOptions();
            }
            if (jarsProcessor == null) {
                jarsProcessor = new SingleUploadFromClassPathJarsProcessor(
                        tmpDir.child("jars"),
                        cacheDir,
                        false,
                        jarsUploadTimeout,
                        fileCacheReplicationFactor
                );
            }
            return new YtClientConfiguration(this);
        }
    }
}
