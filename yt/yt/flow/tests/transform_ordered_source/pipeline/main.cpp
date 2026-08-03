#include <yt/yt/flow/tests/transform_ordered_source/pipeline/proto/event_record.pb.h>

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/parsers/proto.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <util/string/cast.h>
#include <util/system/compiler.h>

#include <fcntl.h>
#include <unistd.h>

namespace NTest {

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

void CrashOnceIfFrontierAdvanced(const std::string& sentinelPath, const TMessageId& initialFrontier, const TMessageId& frontier)
{
    if (sentinelPath.empty() || frontier == initialFrontier) {
        return;
    }
    int sentinelFd = ::open(sentinelPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (sentinelFd < 0) {
        return;
    }
    auto frontierString = std::string(frontier.Underlying());
    Y_UNUSED(::write(sentinelFd, frontierString.data(), frontierString.size()));
    ::_exit(0);
}

////////////////////////////////////////////////////////////////////////////////

class TEventTransform
    : public TTransformOrderedSourceComputation
{
public:
    using TTransformOrderedSourceComputation::TTransformOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        i64 eventId = GetColumnValue<i64>(message, "event_id");
        ui64 reduceId = GetColumnValue<ui64>(message, "reduce_id");
        if (eventId % 7 == 0) {
            return;
        }

        i64 copies = eventId % 5 == 0 ? 2 : 1;
        for (i64 copyIndex = 0; copyIndex < copies; ++copyIndex) {
            auto builder = MakeOutputMessageBuilder();
            builder.Payload().SetValue(MakeUnversionedUint64Value(reduceId), "reduce_id");
            builder.Payload().SetValue(MakeUnversionedInt64Value(eventId), "event_id");
            builder.Payload().SetValue(MakeUnversionedInt64Value(copyIndex), "copy_index");
            builder.Payload().SetValue(MakeUnversionedInt64Value(::getpid()), "worker_pid");
            output->AddMessage(builder.Finish());
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TEventTransform);

////////////////////////////////////////////////////////////////////////////////

constexpr i64 NotDistributedCopyIndex = 99;

class TDistributeFilteredEventTransform
    : public TEventTransform
{
public:
    using TEventTransform::TEventTransform;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) final
    {
        TEventTransform::DoProcessMessage(message, output);

        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(MakeUnversionedUint64Value(GetColumnValue<ui64>(message, "reduce_id")), "reduce_id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(GetColumnValue<i64>(message, "event_id")), "event_id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(NotDistributedCopyIndex), "copy_index");
        builder.Payload().SetValue(MakeUnversionedInt64Value(::getpid()), "worker_pid");
        output->AddMessage(builder.Finish(), /*distribute*/ false);
    }
};

YT_FLOW_DEFINE_COMPUTATION(TDistributeFilteredEventTransform);

////////////////////////////////////////////////////////////////////////////////

class TProtoEventTransform
    : public TProtoTransformOrderedSourceComputation<TEventRecordProto>
{
public:
    using TProtoTransformOrderedSourceComputation<TEventRecordProto>::TProtoTransformOrderedSourceComputation;

    void DoProcessProto(TEventRecordProto&& proto, IOutputCollectorPtr output) final
    {
        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(MakeUnversionedUint64Value(proto.reduce_id()), "reduce_id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(proto.event_id()), "event_id");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TProtoEventTransform);

////////////////////////////////////////////////////////////////////////////////

struct TCounterState
    : public NYTree::TYsonStruct
{
    THashMap<std::string, i64> Counts;

    REGISTER_YSON_STRUCT(TCounterState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("counts", &TThis::Counts)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf CounterStateName = "$state";

struct TCountingTransformParameters
    : public TTransformOrderedSourceComputation::TParameters
{
    std::string CrashSentinelPath;

    REGISTER_YSON_STRUCT(TCountingTransformParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("crash_sentinel_path", &TThis::CrashSentinelPath)
            .Default();
    }
};

class TCountingTransform
    : public TTransformOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCountingTransformParameters);

    using TTransformOrderedSourceComputation::TTransformOrderedSourceComputation;

    void DoInit(IJobInitContextPtr initContext) final
    {
        initContext->InitClient(StateClient_, CounterStateName);
        InitialFrontier_ = GetMaxPersistedMessageIdExclusive();
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) final
    {
        ui64 reduceId = GetColumnValue<ui64>(message, "reduce_id");
        auto state = StateClient_.GetState(message->Key);
        i64 count = ++state->Counts[ToString(reduceId)];

        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(MakeUnversionedUint64Value(reduceId), "reduce_id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(count), "count");
        output->AddMessage(builder.Finish());
    }

    void DoSync(IRetryableTransactionPtr transaction) final
    {
        TTransformOrderedSourceComputation::DoSync(transaction);
        CrashOnceIfFrontierAdvanced(GetParameters()->CrashSentinelPath, InitialFrontier_, GetMaxPersistedMessageIdExclusive());
    }

private:
    TMutableStateKeyClient<TCounterState> StateClient_;
    TMessageId InitialFrontier_;
};

YT_FLOW_DEFINE_COMPUTATION(TCountingTransform);

////////////////////////////////////////////////////////////////////////////////

struct TProtoCountingTransformParameters
    : public TProtoTransformSourceComputationParameters
{
    std::string CrashSentinelPath;

    REGISTER_YSON_STRUCT(TProtoCountingTransformParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("crash_sentinel_path", &TThis::CrashSentinelPath)
            .Default();
    }
};

class TProtoCountingTransform
    : public TProtoTransformOrderedSourceComputation<TEventRecordProto>
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TProtoCountingTransformParameters);

    using TProtoTransformOrderedSourceComputation<TEventRecordProto>::TProtoTransformOrderedSourceComputation;

    void DoInit(IJobInitContextPtr initContext) final
    {
        initContext->InitClient(StateClient_, CounterStateName);
        InitialFrontier_ = GetMaxPersistedMessageIdExclusive();
    }

    void DoProcessProto(const TInputMessageConstPtr& inputMessage, TEventRecordProto&& proto, IOutputCollectorPtr output) final
    {
        auto state = StateClient_.GetState(inputMessage->Key);
        i64 count = ++state->Counts[ToString(proto.reduce_id())];

        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(MakeUnversionedUint64Value(proto.reduce_id()), "reduce_id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(count), "count");
        output->AddMessage(builder.Finish());
    }

    void DoSync(IRetryableTransactionPtr transaction) final
    {
        TProtoTransformOrderedSourceComputation<TEventRecordProto>::DoSync(transaction);
        CrashOnceIfFrontierAdvanced(GetParameters()->CrashSentinelPath, InitialFrontier_, GetMaxPersistedMessageIdExclusive());
    }

private:
    TMutableStateKeyClient<TCounterState> StateClient_;
    TMessageId InitialFrontier_;
};

YT_FLOW_DEFINE_COMPUTATION(TProtoCountingTransform);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
