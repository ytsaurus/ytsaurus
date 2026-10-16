#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        for (const auto& streamId : GetSpec()->OutputStreamIds) {
            auto builder = MakeOutputMessageBuilder(streamId);
            auto newSchema = builder.GetSchema();
            auto dataId = newSchema->GetColumnIndexOrThrow("data");
            auto keyId = newSchema->GetColumnIndexOrThrow("key");

            auto dataValue = GetColumn(message, "data");
            builder.Payload().SetValue(dataValue, dataId);
            builder.Payload().Set(ToString(dataValue.Length), keyId);
            output->AddMessage(builder.Finish());
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTableReader);

YT_FLOW_DEFINE_COMPUTATION(TTableReader);

////////////////////////////////////////////////////////////////////////////////

struct TTableWriter
    : public TTransformComputation
{
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        const auto& streamId = *GetSpec()->OutputStreamIds.begin();
        auto spec = GetContext()->StreamSpecStorage->GetSpec(streamId);

        TMessage outputMessage = ConvertMessageToNewSchema(message, spec->Schema, GetContext()->ConverterCache);
        outputMessage.StreamId = streamId;
        output->AddMessage(std::move(outputMessage));
    }

    void DoSync(IRetryableTransactionPtr /*transaction*/) override
    {
        Sleep(TDuration::MilliSeconds(150));
    }
};

DEFINE_REFCOUNTED_TYPE(TTableWriter);

YT_FLOW_DEFINE_COMPUTATION(TTableWriter);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
