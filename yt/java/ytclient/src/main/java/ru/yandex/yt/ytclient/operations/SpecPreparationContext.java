package ru.yandex.yt.ytclient.operations;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

public class SpecPreparationContext {
    private final YPath tmpDir;
    private final YPath cacheDir;
    private final JarsProcessor jarsProcessor;
    private final boolean isLocalMode;
    private final String javaBinary;
    private final List<String> javaOptions;
    private final Duration jarsUploadTimeout;
    private final int fileCacheReplicationFactor;
    private final String version;

    SpecPreparationContext(Builder builder) {
        this.tmpDir = builder.tmpDir;
        this.cacheDir = builder.cacheDir;
        this.jarsProcessor = builder.jarsProcessor;
        this.isLocalMode = builder.isLocalMode;
        this.javaBinary = builder.javaBinary;
        this.javaOptions = builder.javaOptions;
        this.jarsUploadTimeout = builder.jarsUploadTimeout;
        this.fileCacheReplicationFactor = builder.fileCacheReplicationFactor;
        this.version = builder.version;
    }

    public static Builder builder() {
        return new Builder();
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
        return javaOptions;
    }

    public String getVersion() {
        return version;
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        private YPath tmpDir = YPath.simple("//tmp/yt_wrapper/file_storage");
        @Nullable
        private YPath cacheDir = YPath.simple("//tmp/yt_wrapper/file_storage").child("new_cache");;
        @Nullable
        private JarsProcessor jarsProcessor;
        private boolean isLocalMode = false;
        private String javaBinary = "";  // TODO
        private List<String> javaOptions = Arrays.asList(); // TODO
        private Duration jarsUploadTimeout = Duration.ofMinutes(10);
        private int fileCacheReplicationFactor = 10;
        private String version = "java yt client";

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

        public Builder setJavaOptions(List<String> javaOptions) {
            this.javaOptions = javaOptions;
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

        public SpecPreparationContext build() {
            if (jarsProcessor == null) {
                jarsProcessor = new SingleUploadFromClassPathJarsProcessor(
                        tmpDir.child("jars"),
                        cacheDir,
                        false,
                        jarsUploadTimeout,
                        fileCacheReplicationFactor
                );
            }
            return new SpecPreparationContext(this);
        }
    }
}
