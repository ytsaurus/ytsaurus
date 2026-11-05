package tech.ytsaurus.flow.tests.resource;

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
        var context = new PipelineContext();
        context.registerComputation(Computation.builder()
                .setComputationId("mapper")
                .setProcessFunction(new GreetingMapper())
                .build());
        // These are implementation type names, not the resource ids from the pipeline spec.
        context.registerResourceClass("GreetingDependencyResource", GreetingDependencyResource::new);
        context.registerResourceClass("GreetingResource", GreetingResource::new);
        log.info("Registered the mapper computation and the greeting resources");
        FlowApplication.run(args, context);
    }
}
