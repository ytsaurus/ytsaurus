package tech.ytsaurus.flow.jfr;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;

/**
 * Locates the latest complete JFR chunk file on disk.
 * <p>
 * A chunk is considered "complete" only if there are at least 2 chunks in the repository.
 * The most recently modified file is assumed to be still being written to, so the second
 * most recent file is returned as the latest complete chunk.
 */
public class JfrChunkLocator {

    /**
     * Finds the latest complete JFR chunk.
     *
     * @return a {@link Result} of finding complete JFR chunk.
     */
    public Result findLatestCompleteChunk() {
        Result getRepoResult = getRepositoryPath();
        Path repoDir;

        if (getRepoResult instanceof Result.Found) {
            repoDir = ((Result.Found) getRepoResult).chunkPath();
        } else {
            return getRepoResult;
        }

        List<Path> jfrFiles = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(repoDir, "*.jfr")) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    jfrFiles.add(entry);
                }
            }
        } catch (IOException e) {
            return new Result.Error("Failed to list JFR files in repository: " + repoDir, e);
        }

        if (jfrFiles.size() < 2) {
            return new Result.NotFound("No complete .jfr files found in repository: " + repoDir);
        }

        jfrFiles.sort(Comparator.comparingLong((Path p) -> {
            try {
                return Files.getLastModifiedTime(p).toMillis();
            } catch (IOException e) {
                return 0L;
            }
        }).reversed());

        return new Result.Found(jfrFiles.get(1));
    }

    protected Result getRepositoryPath() {
        if (!FlightRecorder.isAvailable()) {
            return new Result.NotFound("FlightRecorder is not available in this JVM");
        }

        List<Recording> activeRecordings = FlightRecorder.getFlightRecorder().getRecordings();

        if (activeRecordings.isEmpty()) {
            return new Result.NotFound("No active JFR recordings");
        }

        if (activeRecordings.size() != 1) {
            return new Result.NotFound("JVM runs with multiple active JFR recordings: " + activeRecordings.size()
                    + ". Can't recognize correct JFR chunk.");
        }

        String repositoryPath = System.getProperty("jdk.jfr.repository");

        if (repositoryPath == null) {
            return new Result.NotFound("JFR repository path not found in JVM arguments");
        }

        Path repoDir = Paths.get(repositoryPath);
        if (!Files.isDirectory(repoDir)) {
            return new Result.NotFound("JFR repository directory does not exist: " + repoDir);
        }

        return new Result.Found(repoDir);
    }

    /**
     * Result of a JFR chunk lookup.
     */
    public sealed interface Result {
        record Found(Path chunkPath) implements Result {
        }

        record NotFound(String reason) implements Result {
        }

        record Error(String reason, Exception cause) implements Result {
        }
    }
}
