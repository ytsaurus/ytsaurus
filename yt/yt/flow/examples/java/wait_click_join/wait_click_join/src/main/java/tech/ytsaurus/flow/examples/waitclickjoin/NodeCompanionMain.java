package tech.ytsaurus.flow.examples.waitclickjoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class NodeCompanionMain {
    private static final Logger log = LoggerFactory.getLogger(NodeCompanionMain.class);

    private NodeCompanionMain() {
    }

    // [BEGIN main]
    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder(NodeCompanionMain.class)
                .run(args);
    }
    // [END main]
}
