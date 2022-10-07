package ru.yandex.yt.ytclient.operations;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.common.DataSize;
import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.common.JavaOptions;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.FileWriter;
import ru.yandex.yt.ytclient.proxy.TransactionalClient;
import ru.yandex.yt.ytclient.request.WriteFile;

@NonNullApi
@NonNullFields
public abstract class MapperOrReducerSpec implements UserJobSpec {
    public static final DataSize DEFAULT_MEMORY_LIMIT = DataSize.fromMegaBytes(512);
    public static final JavaOptions DEFAULT_JAVA_OPTIONS = JavaOptions.empty().withXmx(DEFAULT_MEMORY_LIMIT);

    private static final Logger logger = LoggerFactory.getLogger(MapperOrReducerSpec.class);

    protected final Class<?> mainClazz;
    protected final MapperOrReducer<?, ?> mapperOrReducer;
    protected final Set<YPath> additionalFiles;
    protected final JavaOptions javaOptions;
    protected final DataSize memoryLimit;
    protected final boolean useTmpfs;
    protected final @Nullable DataSize tmpfsSize;
    protected final @Nullable Double cpuLimit;
    protected final @Nullable Long jobTimeLimit;
    protected final @Nullable Integer jobCount;
    protected final Map<String, String> environment;
    protected final List<YPath> layerPaths;
    protected final @Nullable Integer customStatisticsCountLimit;
    protected final @Nullable Double memoryReserveFactor;
    protected final @Nullable String networkProject;
    protected final @Nullable Duration prepareTimeLimit;

    protected MapperOrReducerSpec(Class<?> mainClazz, Builder<?> builder) {
        if (builder.userJob == null) {
            throw new RuntimeException("userJob wasn't set");
        }

        this.mainClazz = mainClazz;
        mapperOrReducer = builder.userJob;
        additionalFiles = builder.additionalFiles;
        javaOptions = builder.javaOptions;
        memoryLimit = builder.memoryLimit;
        useTmpfs = builder.useTmpfs;
        tmpfsSize = builder.tmpfsSize;
        cpuLimit = builder.cpuLimit;
        jobTimeLimit = builder.jobTimeLimit;
        jobCount = builder.jobCount;
        environment = builder.environment;
        layerPaths = builder.layerPaths;
        customStatisticsCountLimit = builder.customStatisticsCountLimit;
        memoryReserveFactor = builder.memoryReserveFactor;
        networkProject = builder.networkProject;
        prepareTimeLimit = builder.prepareTimeLimit;
    }

    public String getMapperOrReducerTitle() {
        return mapperOrReducer.getClass().getName();
    }

    protected static class Resource {
        private final YPath path;
        private final List<String> args;

        public Resource(YPath path, List<String> args) {
            this.path = path;
            this.args = args;
        }
    }

    protected Optional<Resource> detectResourcesUnsafe(
            TransactionalClient yt,
            MapperOrReducer<?, ?> mapperOrReducer,
            SpecPreparationContext context
    ) throws IOException {
        List<String> args = new ArrayList<>();

        if (mapperOrReducer instanceof Serializable) {
            String fileName = GUID.create() + ".serializable";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(mapperOrReducer);
            oos.close();

            byte[] bytes = baos.toByteArray();
            args.add("serializable");

            YPath path = context.getConfiguration().getTmpDir().child(fileName);

            FileWriter writer = yt.writeFile(new WriteFile(path.toString())).join();
            writer.write(bytes);
            writer.readyEvent().join();
            writer.close().join();

            args.add(fileName);

            return Optional.of(new Resource(
                    path.plusAdditionalAttribute("file_name", fileName),
                    args));
        }

        return Optional.empty();
    }

    private Optional<Resource> detectResources(
            TransactionalClient yt, MapperOrReducer<?, ?> mapperOrReducer, SpecPreparationContext context) {
        try {
            return detectResourcesUnsafe(yt, mapperOrReducer, context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String canonizeJavaPath(String javaPath) {
        String[] pathParts = javaPath.split(":");
        List<String> canonicalPathParts = new ArrayList<>(pathParts.length);
        for (String path : pathParts) {
            try {
                canonicalPathParts.add(new File(path).getCanonicalPath());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        return String.join(":", canonicalPathParts);
    }

    @Override
    public YTreeBuilder prepare(
            YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context, int outputTableCount) {
        Set<YPath> files = new HashSet<>(additionalFiles);

        boolean isLocalMode = context.getConfiguration().isLocalMode();
        String classPath;
        String libraryPath = null;

        if (isLocalMode) {
            classPath = canonizeJavaPath(System.getProperty("java.class.path"));
            libraryPath = canonizeJavaPath(System.getProperty("java.library.path"));
        } else {
            Set<YPath> jars = context.getConfiguration().getJarsProcessor().uploadJars(
                    yt.getRootClient(), mapperOrReducer, isLocalMode);
            files.addAll(jars);
            List<String> jarFileNames = jars.stream()
                    .map(x -> x.getAdditionalAttribute("file_name")
                            .map(YTreeNode::stringValue)
                            .orElseGet(x::name))
                    .collect(Collectors.toList());

            classPath = String.join(":", jarFileNames);
        }

        Set<YPath> autoDetectedResources = context.getConfiguration().getJarsProcessor().uploadResources(
                yt.getRootClient(), mapperOrReducer);
        files.addAll(autoDetectedResources);

        Optional<Resource> resource = detectResources(yt, mapperOrReducer, context);

        List<String> args = new ArrayList<>();
        args.add(String.valueOf(outputTableCount));

        if (!resource.isPresent()) {
            args.add("simple");
            args.add(JavaYtRunner.normalizeClassName(mapperOrReducer.getClass().getName()));
        } else {
            args.addAll(resource.get().args);
            files.add(resource.get().path);
        }

        String javaBinary = context.getConfiguration().getJavaBinary();
        JavaOptions resultJavaOptions = JavaOptions.empty();

        for (String option : context.getConfiguration().getJavaOptions()) {
            resultJavaOptions = resultJavaOptions.withOption(option);
        }

        for (String option : this.javaOptions.getOptions()) {
            resultJavaOptions = resultJavaOptions.withOption(option);
        }

        return builder.beginMap()
                .key("command").value(
                        JavaYtRunner.command(javaBinary, classPath, libraryPath, resultJavaOptions,
                                mainClazz.getName(), args)
                )
                .key("input_format").value(mapperOrReducer.inputType().format())
                .key("output_format").value(mapperOrReducer.outputType().format())
                .key("file_paths").value(files, (b, t) ->
                        b.apply(t::toTree)
                )
                .key("memory_limit").value(memoryLimit.toBytes())
                .when(memoryReserveFactor != null, b -> b.key("memory_reserve_factor").value(memoryReserveFactor))
                .when(useTmpfs, b -> b.key("tmpfs_path").value(".").key("copy_files").value(true))
                .when(tmpfsSize != null, b -> b.key("tmpfs_size").value(tmpfsSize.toBytes()))
                .when(cpuLimit != null, b -> b.key("cpu_limit").value(cpuLimit))
                .when(jobTimeLimit != null, b -> b.key("job_time_limit").value(jobTimeLimit))
                .when(jobCount != null, b -> b.key("job_count").value(jobCount))
                .key("environment").value(environment)
                .key("layer_paths").value(layerPaths.stream().map(YPath::toTree).collect(Collectors.toList()))
                .when(customStatisticsCountLimit != null, b -> b.key("custom_statistics_count_limit")
                        .value(customStatisticsCountLimit))
                .when(networkProject != null, b -> b.key("network_project").value(networkProject))
                .when(prepareTimeLimit != null, b -> b.key("prepare_time_limit")
                        .value(prepareTimeLimit.toMillis()))
                .endMap();
    }

    @NonNullApi
    @NonNullFields
    public abstract static class Builder<T extends Builder<T>> {
        @Nullable MapperOrReducer<?, ?> userJob = null;
        Set<YPath> additionalFiles = Collections.emptySet();
        JavaOptions javaOptions = DEFAULT_JAVA_OPTIONS;
        DataSize memoryLimit = DEFAULT_MEMORY_LIMIT;
        boolean useTmpfs = false;
        @Nullable DataSize tmpfsSize = null;
        @Nullable Double cpuLimit = null;
        @Nullable Long jobTimeLimit = null;
        @Nullable Integer jobCount = null;
        Map<String, String> environment = new HashMap<>();
        List<YPath> layerPaths = new ArrayList<>();
        @Nullable Integer customStatisticsCountLimit = null;
        @Nullable Double memoryReserveFactor = null;
        @Nullable String networkProject = null;
        @Nullable Duration prepareTimeLimit = null; // defaults to 45 minutes

        public abstract MapperOrReducerSpec build();

        protected abstract T self();

        protected T setUserJob(MapperOrReducer<?, ?> userJob) {
            this.userJob = userJob;
            return self();
        }

        protected @Nullable MapperOrReducer<?, ?> getUserJob() {
            return userJob;
        }

        public T setAdditionalFiles(Set<YPath> additionalFiles) {
            this.additionalFiles = additionalFiles;
            return self();
        }

        public T setJavaOptions(JavaOptions javaOptions) {
            this.javaOptions = javaOptions;
            return self();
        }

        public T setMemoryLimit(DataSize memoryLimit) {
            this.memoryLimit = memoryLimit;
            return self();
        }

        public T setUseTmpfs(boolean useTmpfs) {
            this.useTmpfs = useTmpfs;
            return self();
        }

        public T setTmpfsSize(@Nullable DataSize tmpfsSize) {
            this.tmpfsSize = tmpfsSize;
            return self();
        }

        public T setCpuLimit(@Nullable Double cpuLimit) {
            this.cpuLimit = cpuLimit;
            return self();
        }

        public T setJobTimeLimit(@Nullable Long jobTimeLimit) {
            this.jobTimeLimit = jobTimeLimit;
            return self();
        }

        public T setJobCount(@Nullable Integer jobCount) {
            this.jobCount = jobCount;
            return self();
        }

        public T setEnvironment(Map<String, String> environment) {
            this.environment = environment;
            return self();
        }

        public T setLayerPaths(List<YPath> layerPaths) {
            this.layerPaths = layerPaths;
            return self();
        }

        public T setCustomStatisticsCountLimit(@Nullable Integer customStatisticsCountLimit) {
            this.customStatisticsCountLimit = customStatisticsCountLimit;
            return self();
        }

        /**
         * Set memory reserve factor.
         * https://yt.yandex-team.ru/docs/description/mr/operations_options#memory_reserve_factor
         *
         * @param memoryReserveFactor memory reserve factor, if set to null default value (0.5) will be used.
         */
        public T setMemoryReserveFactor(@Nullable Double memoryReserveFactor) {
            this.memoryReserveFactor = memoryReserveFactor;
            return self();
        }

        public T setNetworkProject(@Nullable String networkProject) {
            this.networkProject = networkProject;
            return self();
        }

        public T setPrepareTimeLimit(@Nullable Duration prepareTimeLimit) {
            this.prepareTimeLimit = prepareTimeLimit;
            return self();
        }
    }

    public boolean trackIndices() {
        return mapperOrReducer.trackIndices();
    }
}
