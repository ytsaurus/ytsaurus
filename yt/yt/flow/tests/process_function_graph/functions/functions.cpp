#include "functions.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow::NProcessFunctionGraphTest {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TKeyMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
}

void TStageMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("branch_count", &TThis::BranchCount)
        .Default();
    registrar.Parameter("kind", &TThis::Kind)
        .Default();
}

void TFinalMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("count", &TThis::Count)
        .Default();
    registrar.Parameter("branch_count", &TThis::BranchCount)
        .Default();
    registrar.Parameter("kind", &TThis::Kind)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

// The functions below are referenced by their registered names in the pipeline spec only.

class TQueueReaderFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto result = New<TKeyMessage>();
        result->Key = GetColumnValue<ui64>(message, "key");
        output->AddMessage(context->ConvertToMessage(result));
    }
};

struct TCounterParameters
    : public TYsonStruct
{
    i64 Increment = 1;

    REGISTER_YSON_STRUCT(TCounterParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("increment", &TThis::Increment)
            .Default(1)
            .GreaterThan(0);
    }
};

struct TCounterDynamicParameters
    : public TYsonStruct
{
    i64 Multiplier = 1;

    REGISTER_YSON_STRUCT(TCounterDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("multiplier", &TThis::Multiplier)
            .Default(1)
            .GreaterThan(0);
    }
};

//! Counts messages per key and emits the count for every message and for a timer after it.
class TStateAndTimerFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        Increment_ = context->GetParameters<TCounterParameters>()->Increment;
        context->InitClient<i64>(Count_, "count");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = Count_.GetState(message->Key);
        *state = *state + Increment_ * context->GetDynamicParameters<TCounterDynamicParameters>()->Multiplier;

        auto result = New<TStageMessage>();
        result->Key = GetColumnValue<ui64>(message, "key");
        result->BranchCount = *state;
        result->Kind = 0;
        output->AddMessage(context->ConvertToMessage(result));

        auto triggerTimestamp = TSystemTimestamp(context->GetCurrentTimestamp().Underlying() + 1);
        output->AddTimer(triggerTimestamp, message->EventTimestamp);
    }

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = Count_.GetState(timer->Key);

        auto result = New<TStageMessage>();
        result->Key = GetColumnValue<ui64>(timer, "key");
        result->BranchCount = *state;
        result->Kind = 1;
        output->AddMessage(context->ConvertToMessage(result));
    }

private:
    i64 Increment_ = 1;
    TMutableStateKeyClient<i64> Count_;
};

//! Counts the messages of both branches per key.
class TMergeFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        context->InitClient<i64>(Count_, "count");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = Count_.GetState(message->Key);
        *state = *state + 1;

        auto result = New<TFinalMessage>();
        result->Key = GetColumnValue<ui64>(message, "key");
        result->Count = *state;
        result->BranchCount = GetColumnValue<i64>(message, "branch_count");
        result->Kind = GetColumnValue<i64>(message, "kind");
        output->AddMessage(context->ConvertToMessage(result));
    }

private:
    TMutableStateKeyClient<i64> Count_;
};

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TKeyMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TStageMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TFinalMessage);

YT_FLOW_DEFINE_PROCESS_FUNCTION(TQueueReaderFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TStateAndTimerFunction, TCounterParameters, TCounterDynamicParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TMergeFunction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NProcessFunctionGraphTest
