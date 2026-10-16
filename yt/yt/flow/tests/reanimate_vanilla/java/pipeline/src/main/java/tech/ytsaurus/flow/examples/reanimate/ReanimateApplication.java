package tech.ytsaurus.flow.examples.reanimate;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class ReanimateApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(ReanimateApplication.class)
                .run(args);
    }
}
