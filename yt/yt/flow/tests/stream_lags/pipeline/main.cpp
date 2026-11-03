#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

namespace NYT::NFlow::NStreamLagsTest {

////////////////////////////////////////////////////////////////////////////////

//! A message of the streams `events` and `fired`.
struct TEventMessage
    : public TYsonMessage
{
    ui64 Id = 0;

    REGISTER_YSON_STRUCT(TEventMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("id", &TThis::Id)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TEventMessage);

////////////////////////////////////////////////////////////////////////////////

const TStreamId FarTimerStreamId("far_timer");
const TStreamId NearTimerStreamId("near_timer");

constexpr ui64 FarTimerDelaySeconds = 3600;
constexpr ui64 NearTimerDelaySeconds = 3;
constexpr int NearTimerCount = 10;

//! Registers one timer that is never due during the test and several near ones; near timers emit the event.
class TSchedulerFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto now = context->GetCurrentTimestamp().Underlying();
        output->AddTimer(FarTimerStreamId, TSystemTimestamp(now + FarTimerDelaySeconds), message->EventTimestamp);

        // Distinct trigger timestamps keep the near timers of one key from being deduplicated.
        for (int index = 0; index < NearTimerCount; ++index) {
            output->AddTimer(NearTimerStreamId, TSystemTimestamp(now + NearTimerDelaySeconds + index), message->EventTimestamp);
        }
    }

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (timer->StreamId != NearTimerStreamId) {
            return;
        }

        auto result = New<TEventMessage>();
        result->Id = GetColumnValue<ui64>(timer, "id");
        output->AddMessage(context->ConvertToMessage(result));
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TSchedulerFunction);

////////////////////////////////////////////////////////////////////////////////

void PrepareSpec(const TPipelineSpecPtr& spec, const TDynamicPipelineSpecPtr& dynamicSpec)
{
    TSimpleSpecBuilder streamBuilder;
    streamBuilder.RegisterStream<TEventMessage>("events");
    streamBuilder.RegisterStream<TEventMessage>("fired");
    streamBuilder(spec, dynamicSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStreamLagsTest

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram(&NYT::NFlow::NStreamLagsTest::PrepareSpec)
        .Run(argc, argv);
}
