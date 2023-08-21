package tech.ytsaurus.client.operations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.FileWriter;
import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetFileFromCache;
import tech.ytsaurus.client.request.GetFileFromCacheResult;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.MoveNode;
import tech.ytsaurus.client.request.PutFileToCache;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * Default implementation of {@link JarsProcessor}.
 * Upload jars and files to YT if it is necessary.
 */
@NonNullApi
@NonNullFields
public class SingleUploadFromClassPathJarsProcessor implements JarsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUploadFromClassPathJarsProcessor.class);

    private static final String NATIVE_FILE_EXTENSION = "so";
    protected static final int DEFAULT_JARS_REPLICATION_FACTOR = 10;

    private final YPath jarsDir;
    @Nullable
    protected final YPath cacheDir;
    private final int fileCacheReplicationFactor;

    private final Duration uploadTimeout;
    private final boolean uploadNativeLibraries;
    private final Map<String, YPath> uploadedJars = new HashMap<>();
    private final Map<String, Supplier<InputStream>> uploadMap = new HashMap<>();
    @Nullable
    private volatile Instant lastUploadTime;

    private static final char[] DIGITS = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    public SingleUploadFromClassPathJarsProcessor(YPath jarsDir, @Nullable YPath cacheDir) {
        this(jarsDir, cacheDir, false, Duration.ofMinutes(10), DEFAULT_JARS_REPLICATION_FACTOR);
    }

    public SingleUploadFromClassPathJarsProcessor(YPath jarsDir, @Nullable YPath cacheDir,
                                                  boolean uploadNativeLibraries) {
        this(jarsDir, cacheDir, uploadNativeLibraries, Duration.ofMinutes(10), DEFAULT_JARS_REPLICATION_FACTOR);
    }

    public SingleUploadFromClassPathJarsProcessor(
            YPath jarsDir,
            @Nullable YPath cacheDir,
            boolean uploadNativeLibraries,
            Duration uploadTimeout) {
        this(jarsDir, cacheDir, uploadNativeLibraries, uploadTimeout, DEFAULT_JARS_REPLICATION_FACTOR);
    }

    public SingleUploadFromClassPathJarsProcessor(
            YPath jarsDir,
            @Nullable YPath cacheDir,
            boolean uploadNativeLibraries,
            Duration uploadTimeout,
            @Nullable Integer fileCacheReplicationFactor) {
        this.jarsDir = jarsDir;
        this.cacheDir = cacheDir;
        this.uploadTimeout = uploadTimeout;
        this.uploadNativeLibraries = uploadNativeLibraries;
        this.fileCacheReplicationFactor = fileCacheReplicationFactor != null
                ? fileCacheReplicationFactor
                : DEFAULT_JARS_REPLICATION_FACTOR;
    }

    @Override
    public Set<YPath> uploadJars(TransactionalClient yt, MapperOrReducer<?, ?> mapperOrReducer, boolean isLocalMode) {
        synchronized (this) {
            try {
                uploadIfNeeded(yt.getRootClient(), isLocalMode);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return Set.copyOf(new HashSet<>(uploadedJars.values()));
        }
    }

    protected void withJar(File jarFile, Consumer<File> consumer) {
        consumer.accept(jarFile);
    }

    protected void withClassPathDir(File classPathItem, byte[] jarBytes, BiConsumer<File, byte[]> consumer) {
        consumer.accept(classPathItem, jarBytes);
    }

    private boolean isUsingFileCache() {
        return cacheDir != null;
    }

    private void uploadIfNeeded(TransactionalClient yt, boolean isLocalMode) {
        uploadMap.clear();

        yt.createNode(CreateNode.builder()
                .setPath(jarsDir)
                .setType(CypressNodeType.MAP)
                .setRecursive(true)
                .setIgnoreExisting(true)
                .build()).join();

        if (!isUsingFileCache() && lastUploadTime != null && Instant.now()
                .isBefore(Objects.requireNonNull(lastUploadTime).plus(uploadTimeout))) {
            return;
        }

        uploadedJars.clear();

        collectJars(yt);
        if (uploadNativeLibraries) {
            collectNativeLibs();
        }
        doUpload(yt, isLocalMode);
    }

    protected void writeFile(TransactionalClient yt, YPath path, InputStream data) {
        yt.createNode(new CreateNode(path, CypressNodeType.FILE)).join();
        FileWriter writer = yt.writeFile(WriteFile.builder()
                .setPath(path.toString())
                .setComputeMd5(true)
                .build()).join();
        try {
            byte[] bytes = new byte[0x10000];
            for (; ; ) {
                int count = data.read(bytes);
                if (count < 0) {
                    break;
                }

                writer.write(bytes, 0, count);
                writer.readyEvent().join();
            }
            writer.close().join();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private YPath onFileChecked(TransactionalClient yt, @Nullable YPath path, String originalName, String md5,
                                Supplier<InputStream> fileContent) {
        YPath res = path;
        Objects.requireNonNull(cacheDir);

        if (res == null) {
            YPath tmpPath = jarsDir.child(GUID.create().toString());

            LOGGER.info("Uploading {} to cache", originalName);

            writeFile(yt, tmpPath, fileContent.get());

            res = yt.putFileToCache(new PutFileToCache(tmpPath, cacheDir, md5)).join().getPath();
            yt.removeNode(RemoveNode.builder().setPath(tmpPath).setRecursive(false).setForce(true).build()).join();
        }

        res = res
                .plusAdditionalAttribute("file_name", originalName)
                .plusAdditionalAttribute("md5", md5)
                .plusAdditionalAttribute("cache", cacheDir.toTree());

        return res;
    }

    @NonNullFields
    @NonNullApi
    protected static class CacheUploadTask {
        final CompletableFuture<Optional<YPath>> cacheCheckResult;
        final String md5;
        final Map.Entry<String, Supplier<InputStream>> entry;
        @Nullable
        Future<YPath> result;

        public CacheUploadTask(
                CompletableFuture<Optional<YPath>> cacheCheckResult,
                String md5,
                Map.Entry<String, Supplier<InputStream>> entry
        ) {
            this.cacheCheckResult = cacheCheckResult;
            this.md5 = md5;
            this.entry = entry;
        }
    }

    protected List<CacheUploadTask> checkInCache(TransactionalClient yt, Map<String, Supplier<InputStream>> uploadMap) {
        Objects.requireNonNull(cacheDir);
        List<CacheUploadTask> tasks = new ArrayList<>();
        for (Map.Entry<String, Supplier<InputStream>> entry : uploadMap.entrySet()) {
            String md5 = calculateMd5(entry.getValue().get());
            CompletableFuture<Optional<YPath>> future =
                    yt.getFileFromCache(new GetFileFromCache(cacheDir, md5))
                            .thenApply(GetFileFromCacheResult::getPath);
            tasks.add(new CacheUploadTask(future, md5, entry));
        }
        return tasks;
    }

    private void checkInCacheAndUpload(TransactionalClient yt, Map<String, Supplier<InputStream>> uploadMap) {
        List<CacheUploadTask> tasks = checkInCache(yt, uploadMap);

        int threadsCount = Math.min(uploadMap.size(), 5);
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        try {
            for (CacheUploadTask task : tasks) {
                task.result = executor.submit(() -> {
                    try {
                        Optional<YPath> path = task.cacheCheckResult.get();
                        return onFileChecked(yt, path.orElse(null), task.entry.getKey(), task.md5,
                                task.entry.getValue());
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
            }

            for (CacheUploadTask task : tasks) {
                try {
                    // N.B. we filled `result` in the loop above.
                    Future<YPath> result = Objects.requireNonNull(task.result);
                    uploadedJars.put(task.entry.getKey(), result.get());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    static class UploadTask {
        Future<YPath> result;
        String fileName;

        UploadTask(String fileName) {
            this.result = new CompletableFuture<>();
            this.fileName = fileName;
        }
    }

    private void uploadToTemp(
            TransactionalClient yt,
            Map<String, Supplier<InputStream>> uploadMap,
            boolean isLocalMode
    ) {
        int threadsCount = Math.min(uploadMap.size(), 5);
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        try {
            ArrayList<UploadTask> uploadTasks = new ArrayList<>();
            for (Map.Entry<String, Supplier<InputStream>> entry : uploadMap.entrySet()) {
                String fileName = entry.getKey();
                UploadTask task = new UploadTask(fileName);
                task.result = executor.submit(() -> maybeUpload(yt, entry.getValue(), fileName, isLocalMode));
                uploadTasks.add(task);
            }

            for (UploadTask uploadTask : uploadTasks) {
                try {
                    YPath path = uploadTask.result.get();
                    uploadedJars.put(uploadTask.fileName, path);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    private void doUpload(TransactionalClient yt, boolean isLocalMode) {
        if (uploadMap.isEmpty()) {
            return;
        }

        if (isUsingFileCache()) {
            checkInCacheAndUpload(yt, uploadMap);
        } else {
            uploadToTemp(yt, uploadMap, isLocalMode);
        }

        lastUploadTime = Instant.now();
    }

    private static void walk(File dir, Consumer<File> consumer) {
        consumer.accept(dir);
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            walk(file, consumer);
        }
    }

    private File getParentFile(File file) {
        File parent = file.getParentFile();
        if (parent != null) {
            return parent;
        } else {
            String path = file.getPath();
            if (!path.contains("/") && !path.contains(".")) {
                return new File(".");
            }
            throw new RuntimeException(this + " has no parent");
        }
    }

    private static String toHex(byte[] data) {
        StringBuilder result = new StringBuilder();
        for (byte b : data) {
            result.append(DIGITS[(0xF0 & b) >>> 4]);
            result.append(DIGITS[0x0F & b]);
        }
        return result.toString();
    }

    protected static String calculateMd5(InputStream stream) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = new byte[0x1000];
            for (; ; ) {
                int len = stream.read(bytes);
                if (len < 0) {
                    break;
                }
                md.update(bytes, 0, len);
            }

            return toHex(md.digest());
        } catch (NoSuchAlgorithmException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void collectJars(TransactionalClient yt) {
        yt.createNode(CreateNode.builder()
                .setPath(jarsDir)
                .setType(CypressNodeType.MAP)
                .setRecursive(true)
                .setIgnoreExisting(true)
                .build()).join();
        if (isUsingFileCache()) {
            yt.createNode(CreateNode.builder()
                    .setPath(cacheDir)
                    .setType(CypressNodeType.MAP)
                    .setRecursive(true)
                    .setIgnoreExisting(true)
                    .build()
            ).join();
        }

        Set<String> existsJars = yt.listNode(new ListNode(jarsDir)).join().asList().stream()
                .map(YTreeNode::stringValue)
                .collect(Collectors.toSet());

        if (!isUsingFileCache() && !uploadedJars.isEmpty()) {
            if (uploadedJars.values().stream().allMatch(p -> existsJars.contains(p.name()))) {
                return;
            }
        }

        Set<String> classPathParts = getClassPathParts();
        for (String classPathPart : classPathParts) {
            File classPathItem = new File(classPathPart);
            if (fileHasExtension(classPathItem, "jar")) {
                if (!classPathItem.exists()) {
                    throw new IllegalStateException("Can't find " + classPathItem);
                }
                if (classPathItem.isFile()) {
                    withJar(
                            classPathItem,
                            jar -> collectFile(() -> {
                                try {
                                    return new FileInputStream(jar);
                                } catch (FileNotFoundException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }, classPathItem.getName(), existsJars));
                }
            } else if (classPathItem.isDirectory()) {
                byte[] jarBytes = getClassPathDirJarBytes(classPathItem);
                withClassPathDir(
                        classPathItem,
                        jarBytes,
                        (dir, bytes) -> collectFile(() ->
                                new ByteArrayInputStream(bytes), dir.getName() + ".jar", existsJars
                        )
                );
            }
        }
    }

    private static boolean fileHasExtension(File file, String extension) {
        String lowerExtension = "." + extension;
        return file.getName().toLowerCase().endsWith(lowerExtension);
    }

    private void collectNativeLibs() {
        String libPath = System.getProperty("java.library.path");
        if (libPath == null) {
            throw new IllegalStateException("System property 'java.library.path' is null");
        }
        LOGGER.info("Searching native libs in " + libPath);

        String[] classPathParts = libPath.split(File.pathSeparator);
        for (String classPathPart : classPathParts) {
            File classPathItem = new File(classPathPart);
            if (classPathItem.isDirectory()) {
                walk(classPathItem, elm -> {
                    if (elm.isFile() &&
                            !Files.isSymbolicLink(elm.toPath()) &&
                            fileHasExtension(elm, NATIVE_FILE_EXTENSION)
                    ) {
                        withJar(elm, dll -> collectFile(
                                () -> {
                                    try {
                                        return new FileInputStream(dll);
                                    } catch (FileNotFoundException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                },
                                dll.getName(),
                                Collections.emptySet()));
                    }
                });
            }
        }
    }

    /**
     * @return set of classpath files (usually *.jar)
     */
    private Set<String> getClassPathParts() {
        Set<String> classPathParts = new HashSet<>();
        String classPath = System.getProperty("java.class.path");
        if (classPath == null) {
            throw new IllegalStateException("System property 'java.class.path' is null");
        }
        LOGGER.info("Searching libs in " + classPath);

        String[] classPathPartsRaw = classPath.split(File.pathSeparator);

        Attributes.Name classPathKey = new Attributes.Name("Class-Path");
        for (String classPathPart : classPathPartsRaw) {
            classPathParts.add(classPathPart);

            try {
                File jarFile = new File(classPathPart);
                Manifest m = new JarFile(classPathPart).getManifest();
                if (m != null) {
                    Attributes a = m.getMainAttributes();
                    if (a.containsKey(classPathKey)) {
                        String[] fileList = a.getValue(classPathKey).split(" ");

                        for (String entity : fileList) {
                            try {
                                File jarFileChild;
                                if (entity.startsWith("file:")) {
                                    jarFileChild = new File(new URI(entity));
                                } else {
                                    jarFileChild = new File(entity);
                                }
                                if (!jarFileChild.isAbsolute()) {
                                    jarFileChild = new File(getParentFile(jarFile), entity);
                                }

                                if (jarFileChild.exists()) {
                                    classPathParts.add(jarFileChild.getPath());
                                }
                            } catch (Throwable e) {
                                LOGGER.warn("Cannot open : {}", entity, e);
                            }
                        }
                    }
                }
            } catch (IOException ignored) {
            }
        }
        return classPathParts;
    }

    private static String calculateYPath(Supplier<InputStream> fileContent, String originalName) {
        String md5 = calculateMd5(fileContent.get());
        String[] parts = originalName.split("\\.");
        String ext = parts.length < 2 ? "" : parts[parts.length - 1];

        return md5 + "." + ext;
    }

    private void collectFile(Supplier<InputStream> fileContent, String originalName, Set<String> existsFiles) {
        String fileName = calculateYPath(fileContent, originalName);
        boolean exists = existsFiles.contains(fileName);
        if (isUsingFileCache() || !exists) {
            if (!uploadMap.containsKey(originalName)) {
                uploadMap.put(originalName, fileContent);
            } else if (originalName.endsWith(".jar")) {
                String baseName = originalName.split("\\.")[0];
                uploadMap.put(baseName + "-" + calculateMd5(fileContent.get()) + ".jar", fileContent);
            }
        }
        if (!isUsingFileCache() && exists) {
            uploadedJars.put(originalName, jarsDir.child(fileName));
        }
    }

    private YPath maybeUpload(
            TransactionalClient yt,
            Supplier<InputStream> fileContent,
            String originalName,
            boolean isLocalMode
    ) {
        String md5 = calculateMd5(fileContent.get());
        YPath jarPath;
        if (originalName.endsWith(NATIVE_FILE_EXTENSION)) {
            // TODO: do we really need this?
            YPath dllDir = jarsDir.child(md5);
            yt.createNode(CreateNode.builder()
                    .setPath(dllDir)
                    .setType(CypressNodeType.MAP)
                    .setRecursive(true)
                    .setIgnoreExisting(true)
                    .build()).join();
            jarPath = dllDir.child(originalName);
        } else {
            jarPath = jarsDir.child(calculateYPath(fileContent, originalName));
        }

        YPath tmpPath = jarsDir.child(GUID.create().toString());

        LOGGER.info("Uploading {} as {} using tmpPath {}", originalName, jarPath, tmpPath);

        int actualFileCacheReplicationFactor = isLocalMode ? 1 : fileCacheReplicationFactor;

        yt.createNode(CreateNode.builder()
                .setPath(tmpPath)
                .setType(CypressNodeType.FILE)
                .addAttribute("replication_factor", YTree.integerNode(actualFileCacheReplicationFactor))
                .setIgnoreExisting(true)
                .build()).join();

        writeFile(yt, tmpPath, fileContent.get());

        yt.moveNode(MoveNode.builder()
                .setSource(tmpPath.toString())
                .setDestination(jarPath.toString())
                .setPreserveAccount(true)
                .setRecursive(true)
                .setForce(true)
                .build()).join();

        return jarPath.plusAdditionalAttribute("file_name", originalName);
    }

    private static byte[] getClassPathDirJarBytes(File dir) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            JarOutputStream jar = new JarOutputStream(bytes) {
                @Override
                public void putNextEntry(ZipEntry ze) throws IOException {
                    // makes resulting jar md5 predictable to allow jar hashing at yt side
                    // https://stackoverflow.com/questions/26525936
                    ze.setTime(-1);
                    super.putNextEntry(ze);
                }
            };
            walk(dir, elm -> {
                String name = elm.getAbsolutePath().substring(dir.getAbsolutePath().length());
                if (name.length() > 0) {
                    try {
                        JarEntry entry = new JarEntry(name.substring(1).replace("\\", "/"));
                        jar.putNextEntry(entry);
                        if (elm.isFile()) {
                            Files.copy(elm.toPath(), jar);
                        }
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            });
            jar.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return bytes.toByteArray();
    }
}
