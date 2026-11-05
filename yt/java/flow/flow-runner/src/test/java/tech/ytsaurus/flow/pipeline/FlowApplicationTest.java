package tech.ytsaurus.flow.pipeline;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.FlowRunMode;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.testutils.MockEnvironmentReader;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowApplicationTest {

    @Test
    void testControllerModeIsRejected() {
        var env = new MockEnvironmentReader();
        env.setVar(EnvironmentReader.ENV_VAR_FLOW_MODE, FlowRunMode.Controller.name());

        var error = assertThrows(
                IllegalStateException.class,
                () -> FlowApplication.run(new String[]{}, new PipelineContext(), env));
        assertTrue(error.getMessage().contains("Controller mode is not supported"));
    }

    @Test
    void testWorkerModeRequiresCompanionConfig() {
        var env = new MockEnvironmentReader().worker();

        // The companion branch resolves its config from the environment; without
        // YT_FLOW_COMPANION_CONFIG it fails before the gRPC server is bound.
        var error = assertThrows(
                IllegalArgumentException.class,
                () -> FlowApplication.run(new String[]{}, new PipelineContext(), env));
        assertTrue(error.getMessage().contains("YT_FLOW_COMPANION_CONFIG"));
    }

    @Test
    void testRunnerModeRequiresConfigArgument() {
        var env = new MockEnvironmentReader();

        // Runner mode: no YT_FLOW_MODE. The launch fails on the missing --config, which proves the
        // runner branch was taken rather than the companion one.
        assertThrows(
                ParameterException.class,
                () -> FlowApplication.run(new String[]{}, new PipelineContext(), env));
    }

}
