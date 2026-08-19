package tech.ytsaurus.flow.examples.urldownloader;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class PipelineMain {

    private PipelineMain() {
    }

    // [BEGIN main]
    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder(PipelineMain.class).run(args);
    }
    // [END main]
}
