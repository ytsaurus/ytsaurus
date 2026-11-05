package tech.ytsaurus.flow.pipeline;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleRunnerProgramTest {

    @Test
    void unknownOptionsFailTheParse() {
        // A typo must die at the parser, not be silently dropped from the launch.
        assertThrows(
                ParameterException.class,
                () -> SimpleRunnerProgram.runPipeline(
                        new String[]{"--conifg", "p.yson", "--flow-bin", "fs"}, Map.of()));
    }

    @Test
    void propertyStyleOptionsArePassedOver() throws Exception {
        // Property options arrive on the same command line and must not fail the parse.
        int exitCode = SimpleRunnerProgram.runPipeline(
                new String[]{
                        "--help",
                        "--spring.profiles.active=test",
                        "--server.port=8081",
                        "--logging.level.root=DEBUG",
                },
                Map.of());

        assertEquals(0, exitCode);
    }

    @Test
    void dotlessUnknownOptionsStillFail() {
        // The exemption is keyed on the dot, so a dotless typo keeps failing loudly.
        assertThrows(
                ParameterException.class,
                () -> SimpleRunnerProgram.runPipeline(
                        new String[]{"--debug", "--config", "p.yson", "--flow-bin", "fs"}, Map.of()));
    }

    @Test
    void flowServerFlagsArePassedThrough(@TempDir Path tempDir) throws Exception {
        // --validate-only must reach flow_server, or a dry run would restart a live pipeline.
        Path argsFile = tempDir.resolve("flow_server_args.txt");
        Path fakeFlowServer = recordingFlowServer(tempDir, argsFile);

        int exitCode = SimpleRunnerProgram.runPipeline(
                new String[]{
                        "--config", pipelineConfig(tempDir).toString(),
                        "--flow-bin", fakeFlowServer.toString(),
                        "--validate-only",
                        "--skip-set-flow-core-target",
                },
                Map.of());

        assertEquals(0, exitCode);
        String recorded = Files.readString(argsFile).trim();
        assertTrue(recorded.contains("--validate-only"), recorded);
        assertTrue(recorded.contains("--skip-set-flow-core-target"), recorded);
    }

    @Test
    void noFlagsMeansNoExtras(@TempDir Path tempDir) throws Exception {
        Path argsFile = tempDir.resolve("flow_server_args.txt");
        Path fakeFlowServer = recordingFlowServer(tempDir, argsFile);

        int exitCode = SimpleRunnerProgram.runPipeline(
                new String[]{
                        "--config", pipelineConfig(tempDir).toString(),
                        "--flow-bin", fakeFlowServer.toString(),
                },
                Map.of());

        assertEquals(0, exitCode);
        assertFalse(Files.readString(argsFile).contains("--validate-only"));
    }

    /** A flow_server stand-in that records the argv it was launched with. */
    private static Path recordingFlowServer(Path tempDir, Path argsFile) throws Exception {
        Path fakeFlowServer = tempDir.resolve("flow_server");
        Files.writeString(fakeFlowServer, "#!/bin/sh\necho \"$@\" > " + argsFile + "\n");
        Files.setPosixFilePermissions(fakeFlowServer, PosixFilePermissions.fromString("rwxr-xr-x"));
        return fakeFlowServer;
    }

    private static Path pipelineConfig(Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("pipeline.yson");
        Files.writeString(configPath, """
                {
                    "cluster_url" = "test_cluster";
                    "path" = "//some/yt/path/pipeline";
                    "spec" = {};
                }
                """);
        return configPath;
    }

    @Test
    void flowServerExitCodeIsPropagated(@TempDir Path tempDir) throws Exception {
        // A failing launch must report its exit code, not a constant 0.
        Path fakeFlowServer = tempDir.resolve("flow_server");
        Files.writeString(fakeFlowServer, "#!/bin/sh\nexit 3\n");
        Files.setPosixFilePermissions(fakeFlowServer, PosixFilePermissions.fromString("rwxr-xr-x"));

        int exitCode = SimpleRunnerProgram.runPipeline(
                new String[]{
                        "--config", pipelineConfig(tempDir).toString(),
                        "--flow-bin", fakeFlowServer.toString(),
                },
                Map.of());

        assertEquals(3, exitCode);
    }
}
