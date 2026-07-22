package tech.ytsaurus.flow.tests.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.execution.CompanionExecutionSpec;
import tech.ytsaurus.flow.execution.GrpcServerExecution;

public class NodeCompanionMain {

    private static final Logger log = LoggerFactory.getLogger(NodeCompanionMain.class);

    private NodeCompanionMain() {
    }

    public static void main(String[] args) throws Exception {
        log.info("Starting companion execution");
        Computation mapper = Computation.builder()
                .setComputationId("mapper")
                .setProcessFunction(new TypeMapper())
                .build();
        log.info("Registering computation: {}", mapper);
        var context = new PipelineContext();
        context.registerComputation(mapper);
        GrpcServerExecution execution = new GrpcServerExecution(new CompanionExecutionSpec(context));
        log.info("Starting execution...");
        execution.start();
        log.info("Execution completed");
    }
}
