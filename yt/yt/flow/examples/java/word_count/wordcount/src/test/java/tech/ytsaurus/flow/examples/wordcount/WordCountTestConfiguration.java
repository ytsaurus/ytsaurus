package tech.ytsaurus.flow.examples.wordcount;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.execution.CompanionExecution;
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures;
import tech.ytsaurus.flow.testutils.NoServerTestExecution;

@TestConfiguration
public class WordCountTestConfiguration {

    @Bean
    public CompanionExecutionConfig companionExecutionConfig() {
        return CompanionConfigFixtures.defaults();
    }

    @Bean
    public CompanionExecution grpcServerExecution(
            PipelineContext pipelineContext,
            CompanionExecutionConfig companionExecutionConfig
    ) {
        return new NoServerTestExecution(pipelineContext, companionExecutionConfig);
    }
}
