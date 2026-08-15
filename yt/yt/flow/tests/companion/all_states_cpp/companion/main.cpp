#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

namespace NYT::NFlow::NCompanionTest {

////////////////////////////////////////////////////////////////////////////////

//! Enriches source rows with partition-local sequence numbers and joined metadata.
class TStatefulSourceFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient(Sequence_, "reader-state");
        initContext->InitExternalStateClient(Metadata_, "/word-metadata");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto sequence = Sequence_.GetState(message);
        ++*sequence;

        auto metadata = Metadata_.GetState(message);
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set<std::string>(GetColumnValue<std::string>(message, "word"), "word");
        builder.Payload().Set<i64>(*sequence, "source_sequence");
        builder.Payload().Set<std::string>(metadata->GetColumnValue<std::string>("tag"), "tag");
        output->AddMessage(builder.Finish());
    }

private:
    TMutableStateKeyClient<i64> Sequence_;
    TJoinedStateKeyClient<TSimpleExternalState> Metadata_;
};

////////////////////////////////////////////////////////////////////////////////

//! Counts words per key: increments the internal "word-state" counter, mirrors
//! the count into the external state table, and emits the word downstream on
//! its first occurrence (output uniqueness is asserted by the test). The emitted
//! weight comes from a joiner keyed by "tag", not by the computation's key.
class TWordCountAllStatesFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient(Counter_, "word-state");
        initContext->InitExternalStateClient(External_, "/word-state-external");
        initContext->InitExternalStateClient(TagMetadata_, "/tag-metadata");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto count = Counter_.GetState(message);
        *count += 1;

        auto external = External_.GetState(message->Key);
        {
            TPayloadBuilder builder(external->Schema);
            builder.Set(*count, "count");
            external->Payload = builder.Finish();
        }

        if (*count == 1) {
            auto tagMetadata = TagMetadata_.GetState(message);
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set<std::string>(GetColumnValue<std::string>(message, "word"), "word");
            builder.Payload().Set<i64>(tagMetadata->GetColumnValue<i64>("weight"), "tag_weight");
            output->AddMessage(builder.Finish());
        }
    }

private:
    TMutableStateKeyClient<i64> Counter_;
    TMutableStateKeyClient<TSimpleExternalState> External_;
    TJoinedStateKeyClient<TSimpleExternalState> TagMetadata_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionTest

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddSource<NYT::NFlow::NCompanionTest::TStatefulSourceFunction>("reader");
    pipeline.AddTransform<NYT::NFlow::NCompanionTest::TWordCountAllStatesFunction>("counter");
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
