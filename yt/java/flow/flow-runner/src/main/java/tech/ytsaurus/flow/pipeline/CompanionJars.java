package tech.ytsaurus.flow.pipeline;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Discovers the companion jars shipped next to the runner and ships them into the worker task's
 * {@code local_files}.
 */
class CompanionJars {
    static final String COMPANION_JARS_DIR = "java_companion";

    private static final Logger log = LoggerFactory.getLogger(CompanionJars.class);

    /** Ships every companion jar into {@code worker.local_files}. */
    void ship(YTreeMapNode worker) {
        YTreeMapNode localFiles = getOrCreateMap(worker, "local_files");
        // Seed with the user's own entries so a discovered jar never overwrites them.
        Set<String> usedNames = new HashSet<>(localFiles.asMap().keySet());
        for (Path jar : discover()) {
            String fileName = jar.getFileName().toString();
            String inJobName = Path.of(COMPANION_JARS_DIR, fileName).toString();
            // Distinct jars may share a file name (e.g. two subprojects both emitting proto.jar);
            // ship each under a distinct in-job name so the classpath glob picks all of them up.
            for (int index = 2; !usedNames.add(inJobName); index++) {
                inJobName = Path.of(COMPANION_JARS_DIR, index + "-" + fileName).toString();
            }
            localFiles.put(inJobName, YTree.stringNode(jar.toAbsolutePath().toString()));
        }
        log.info("Shipping {} companion jars under {}", localFiles.size(), COMPANION_JARS_DIR);
    }

    /**
     * Enumerates the companion jars to ship into the vanilla job: the jars under the runner's
     * {@code java.library.path} directories, or, when there are none, the jar entries of
     * {@code java.class.path}.
     */
    protected List<Path> discover() {
        return discover(
                System.getProperty("java.library.path", ""),
                System.getProperty("java.class.path", ""));
    }

    /**
     * The ya-built runner keeps every jar flat under the {@code java.library.path} directories, so
     * they are preferred; a plain {@code java -cp ...} launch has no such directories, so the jar
     * entries of {@code java.class.path} are shipped instead.
     */
    List<Path> discover(String libraryPath, String classPath) {
        Set<Path> jars = new LinkedHashSet<>();
        pathEntries(libraryPath).filter(Files::isDirectory).forEach(dir -> {
            try (Stream<Path> files = Files.list(dir)) {
                files.filter(this::isJar).forEach(jars::add);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        if (jars.isEmpty()) {
            pathEntries(classPath).filter(this::isJar).filter(Files::isRegularFile).forEach(jars::add);
        }
        if (jars.isEmpty()) {
            throw new IllegalStateException(
                    "No companion jars found under java.library.path=" + libraryPath
                            + " or on java.class.path=" + classPath
                            + "; cannot ship the Java companion into the vanilla job");
        }
        return new ArrayList<>(jars);
    }

    /** The non-empty entries of a path-separated list, as absolute paths. */
    private Stream<Path> pathEntries(String pathList) {
        return Arrays.stream(pathList.split(File.pathSeparator))
                .filter(entry -> !entry.isEmpty())
                .map(entry -> Paths.get(entry).toAbsolutePath());
    }

    private boolean isJar(Path path) {
        return path.getFileName().toString().endsWith(".jar");
    }

    private YTreeMapNode getOrCreateMap(YTreeMapNode parent, String key) {
        YTreeNode existing = parent.get(key).orElse(null);
        if (existing != null && existing.isMapNode()) {
            return existing.mapNode();
        }
        YTreeMapNode created = YTree.mapBuilder().buildMap();
        parent.put(key, created);
        return created;
    }
}
