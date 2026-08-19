package tech.ytsaurus.flow.tests.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.pipeline.FlowApplication;

public class PipelineMain {

    private static final Logger log = LoggerFactory.getLogger(PipelineMain.class);

    private PipelineMain() {
    }

    public static void main(String[] args) throws Exception {
        Computation mapper = Computation.builder()
                .setComputationId("mapper")
                .setProcessFunction(new TypeMapper())
                .build();
        log.info("Registering computation: {}", mapper);
        var context = new PipelineContext();
        context.registerComputation(mapper);
        FlowApplication.run(args, context);
    }
}
