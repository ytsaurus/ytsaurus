package tech.ytsaurus.flow.examples.wordcount

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import tech.ytsaurus.flow.config.CompanionExecutionConfig
import tech.ytsaurus.flow.context.PipelineContext
import tech.ytsaurus.flow.execution.CompanionExecution
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures
import tech.ytsaurus.flow.testutils.NoServerTestExecution

@TestConfiguration
open class WordCountTestConfiguration {

    @Bean
    open fun companionExecutionConfig(): CompanionExecutionConfig {
        return CompanionConfigFixtures.defaults()
    }

    @Bean
    open fun grpcServerExecution(
        pipelineContext: PipelineContext,
        companionExecutionConfig: CompanionExecutionConfig,
    ): CompanionExecution {
        return NoServerTestExecution(pipelineContext, companionExecutionConfig)
    }
}
