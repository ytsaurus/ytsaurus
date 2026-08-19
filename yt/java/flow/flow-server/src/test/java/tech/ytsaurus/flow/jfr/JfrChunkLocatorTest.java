package tech.ytsaurus.flow.jfr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JfrChunkLocatorTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("jfr-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            try (var walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                // ignore
                            }
                        });
            }
        }
    }

    @Test
    void emptyDirectoryReturnsNoCompleteChunks() throws IOException {
        var locator = new TestableJfrChunkLocator(tempDir);
        var result = locator.findLatestCompleteChunk();

        assertInstanceOf(JfrChunkLocator.Result.NotFound.class, result);
    }

    @Test
    void singleChunkReturnsNoCompleteChunks() throws IOException {
        Files.createFile(tempDir.resolve("chunk_2024_01_01_00_00_00.jfr"));

        var locator = new TestableJfrChunkLocator(tempDir);
        var result = locator.findLatestCompleteChunk();

        assertInstanceOf(JfrChunkLocator.Result.NotFound.class, result);
        var noChunks = (JfrChunkLocator.Result.NotFound) result;
        assertTrue(noChunks.reason().contains("No complete"));
    }

    @Test
    void twoChunksReturnsOlderOne() throws IOException {
        // Create two chunks with different modification times.
        Path older = Files.createFile(tempDir.resolve("chunk_2024_01_01_00_00_00.jfr"));
        Thread.yield(); // Ensure different timestamps.
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Path newer = Files.createFile(tempDir.resolve("chunk_2024_01_01_00_30_00.jfr"));

        // Write some data to make them distinguishable.
        Files.writeString(older, "older chunk data");
        Files.writeString(newer, "newer chunk data");

        // Ensure newer has a later modification time.
        newer.toFile().setLastModified(System.currentTimeMillis());
        older.toFile().setLastModified(System.currentTimeMillis() - 60_000);

        var locator = new TestableJfrChunkLocator(tempDir);
        var result = locator.findLatestCompleteChunk();

        assertInstanceOf(JfrChunkLocator.Result.Found.class, result);
        var found = (JfrChunkLocator.Result.Found) result;
        assertEquals(older.getFileName().toString(), found.chunkPath().getFileName().toString());
    }

    @Test
    void threeChunksReturnsSecondMostRecent() throws IOException {
        Path oldest = Files.createFile(tempDir.resolve("chunk_001.jfr"));
        Path middle = Files.createFile(tempDir.resolve("chunk_002.jfr"));
        Path newest = Files.createFile(tempDir.resolve("chunk_003.jfr"));

        // Set modification times.
        oldest.toFile().setLastModified(System.currentTimeMillis() - 120_000);
        middle.toFile().setLastModified(System.currentTimeMillis() - 60_000);
        newest.toFile().setLastModified(System.currentTimeMillis());

        var locator = new TestableJfrChunkLocator(tempDir);
        var result = locator.findLatestCompleteChunk();

        assertInstanceOf(JfrChunkLocator.Result.Found.class, result);
        var found = (JfrChunkLocator.Result.Found) result;
        assertEquals(middle.getFileName().toString(), found.chunkPath().getFileName().toString());
    }

    @Test
    void nonJfrFilesAreIgnored() throws IOException {
        Files.createFile(tempDir.resolve("chunk_001.jfr"));
        Files.createFile(tempDir.resolve("readme.txt"));
        Files.createFile(tempDir.resolve("data.log"));

        // Set modification times.
        tempDir.resolve("chunk_001.jfr").toFile().setLastModified(System.currentTimeMillis());

        var locator = new TestableJfrChunkLocator(tempDir);
        var result = locator.findLatestCompleteChunk();

        // Only one .jfr file → NoCompleteChunks.
        assertInstanceOf(JfrChunkLocator.Result.NotFound.class, result);
    }

    @Test
    void nonExistentDirectoryReturnsNoCompleteChunks() {
        var locator = new TestableJfrChunkLocator(Path.of("/nonexistent/path/jfr"));
        var result = locator.findLatestCompleteChunk();

        assertInstanceOf(JfrChunkLocator.Result.NotFound.class, result);
    }

    /**
     * Testable subclass that overrides JFR status check and repository path resolution
     * to allow testing the file scanning logic in isolation.
     */
    private static class TestableJfrChunkLocator extends JfrChunkLocator {
        private final Path repositoryPath;

        TestableJfrChunkLocator(Path repositoryPath) {
            this.repositoryPath = repositoryPath;
        }

        @Override
        public Result getRepositoryPath() {
            return new Result.Found(repositoryPath);
        }

        @Override
        public Result findLatestCompleteChunk() {
            // Skip JFR status check and repository path resolution.
            // Directly scan the provided directory.
            if (!Files.isDirectory(repositoryPath)) {
                return new Result.NotFound(
                        "JFR repository directory does not exist: " + repositoryPath);
            }

            return super.findLatestCompleteChunk();
        }
    }
}
