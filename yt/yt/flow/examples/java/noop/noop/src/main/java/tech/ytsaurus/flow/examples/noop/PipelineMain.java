package tech.ytsaurus.flow.examples.noop;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class PipelineMain {

    private PipelineMain() {
    }

    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder(PipelineMain.class).run(args);
    }
}
