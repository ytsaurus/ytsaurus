package tech.ytsaurus.flow.examples.retryableasyncrequest;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class NodeCompanionMain {

    private NodeCompanionMain() {
    }

    // [BEGIN main]
    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder(NodeCompanionMain.class).run(args);
    }
    // [END main]
}
