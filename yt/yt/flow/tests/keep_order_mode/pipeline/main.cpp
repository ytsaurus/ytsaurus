#include <yt/yt/flow/library/cpp/computation/passthrough_computation.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <library/cpp/codecs/codecs.h>
#include <library/cpp/yson/node/node_io.h>

namespace NExample {

using namespace NYT;
using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NFlow;
using namespace NYT::NLogging;
using namespace NYT::NTableClient;
using namespace NYT::NYson;

void LogMessage(const TLogger& Logger, const TMessage& message)
{
    YT_LOG_INFO("MessageLifeCycle.KeepOrderModeTest: message processing "
        "(MessageId: %v, ReduceId: %v, EventId: %v)",
        message.MessageId,
        GetColumnValue<ui64>(message, "reduce_id"),
        GetColumnValue<i64>(message, "event_id"));
}

////////////////////////////////////////////////////////////////////////////////

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder();
        auto codec = NCodecs::ICodec::GetInstance(GetColumnValue<std::string>(message, "codec"));
        TBuffer buffer;
        codec->Decode(GetColumnValue<std::string>(message, "value"), buffer);
        TNode result = NodeFromYsonString(TStringBuf(buffer.data(), buffer.size()));
        builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(result.ChildConvertTo<ui64>("reduce_id")), "reduce_id");
        builder.Payload().SetValue(NTableClient::MakeUnversionedInt64Value(result.ChildConvertTo<i64>("event_id")), "event_id");
        builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(result.ChildConvertTo<ui64>("event_time")), "event_time");
        auto outputMessage = builder.Finish();
        outputMessage.MessageId = message.MessageId;
        LogMessage(Logger, outputMessage);
        output->AddMessage(std::move(outputMessage));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

class TRacyTransformComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoPrepare(const IComputationRunContextPtr& context) override
    {
        // For test: try to get more races between interrupting and executing partitions.
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(4.0 * RandomNumber<float>()));
        TTransformComputation::DoPrepare(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRacyPassthroughComputation
    : public TRacyTransformComputation
{
public:
    using TRacyTransformComputation::TRacyTransformComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        LogMessage(Logger, message);
        output->AddMessage(ConvertToOutputMessage(message));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TRacyPassthroughComputation);

////////////////////////////////////////////////////////////////////////////////

class TQueueReducer
    : public TRacyTransformComputation
{
public:
    using TRacyTransformComputation::TRacyTransformComputation;

    void DoProcessMessage(
        const TMessage& message,
        IOutputCollectorPtr output) final
    {
        LogMessage(Logger, message);

        i64 eventId = GetColumnValue<i64>(message, "event_id");
        if (eventId == -1) {
            return;
        }

        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(GetColumnValue<ui64>(message, "reduce_id")), "reduce_id");
        builder.Payload().SetValue(NTableClient::MakeUnversionedInt64Value(eventId), "event_id");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TQueueReducer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExample

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
