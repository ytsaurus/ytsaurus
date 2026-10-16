package tech.ytsaurus.flow.spring;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.execution.CompanionExecutionSpec;
import tech.ytsaurus.flow.execution.GrpcServerExecution;
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlowCompanionLifecycle}.
 */
class FlowCompanionLifecycleTest {

    private static CompanionExecutionConfig config() {
        return CompanionConfigFixtures.defaults();
    }

    @Test
    void lifecycleStartsAndStops() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        assertFalse(lifecycle.isRunning());
        assertEquals(-1, lifecycle.getPort());

        lifecycle.start();

        assertTrue(lifecycle.isRunning());
        assertTrue(lifecycle.getPort() > 0, "Port should be assigned after start");

        lifecycle.stop();

        assertFalse(lifecycle.isRunning());
    }

    @Test
    void isAutoStartupReturnsTrue() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        assertTrue(lifecycle.isAutoStartup());
    }

    @Test
    void phaseIsMaxValue() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        assertEquals(Integer.MAX_VALUE, lifecycle.getPhase());
    }

    @Test
    void startIsIdempotent() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        lifecycle.start();
        int port = lifecycle.getPort();

        // Second start should be a no-op
        lifecycle.start();
        assertEquals(port, lifecycle.getPort(), "Port should not change on second start");

        lifecycle.stop();
    }

    @Test
    void stopIsIdempotent() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        lifecycle.start();
        lifecycle.stop();

        // Second stop should be a no-op
        lifecycle.stop();

        assertFalse(lifecycle.isRunning());
    }

    @Test
    void stopWithCallbackInvokesCallback() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        lifecycle.start();

        boolean[] callbackInvoked = {false};
        lifecycle.stop(() -> callbackInvoked[0] = true);

        assertTrue(callbackInvoked[0], "Callback should be invoked");
        assertFalse(lifecycle.isRunning());
    }

    @Test
    void stopWithCallbackInvokesCallbackEvenWhenNotRunning() {
        var context = new PipelineContext();
        var execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(config()));

        FlowCompanionLifecycle lifecycle = new FlowCompanionLifecycle(execution);

        // Don't start, just stop
        boolean[] callbackInvoked = {false};
        lifecycle.stop(() -> callbackInvoked[0] = true);

        assertTrue(callbackInvoked[0], "Callback should be invoked even when not running");
    }
}
