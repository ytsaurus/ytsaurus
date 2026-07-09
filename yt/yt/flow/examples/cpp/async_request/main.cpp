#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

struct TEventMessage
    : public TYsonMessage
{
    ui64 Key{};
    std::string Data;

    REGISTER_YSON_STRUCT(TEventMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("data", &TThis::Data)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TEventMessage);

////////////////////////////////////////////////////////////////////////////////

struct TRequestMessage
    : public TYsonMessage
{
    ui64 RequestId{};
    ui64 Key{};
    std::string Request;

    REGISTER_YSON_STRUCT(TRequestMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("request_id", &TThis::RequestId)
            .Default();
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("request", &TThis::Request)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TRequestMessage);

using TRequestMessagePtr = NYT::TIntrusivePtr<TRequestMessage>;

////////////////////////////////////////////////////////////////////////////////

struct TResponseMessage
    : public TYsonMessage
{
    ui64 RequestId{};
    ui64 Key{};
    i64 Length{};

    REGISTER_YSON_STRUCT(TResponseMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("request_id", &TThis::RequestId)
            .Default();
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("length", &TThis::Length)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TResponseMessage);

////////////////////////////////////////////////////////////////////////////////

struct TDelayedRequestState
    : public NYTree::TYsonStruct
{
    ui64 FailedAttempts{};
    TRequestMessagePtr Request;

    REGISTER_YSON_STRUCT(TDelayedRequestState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("failed_attempts", &TThis::FailedAttempts)
            .Default();
        registrar.Parameter("request", &TThis::Request)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

// [BEGIN request_processor]
class TRequestProcessor
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        const auto ysonMessage = ConvertToYsonMessage(message);
        auto request = ysonMessage->As<TRequestMessage>();
        auto response = New<TResponseMessage>();
        response->RequestId = request->RequestId;
        response->Key = request->Key;
        response->Length = std::ssize(request->Request);
        output->AddMessage(ConvertToMessage(response));
        YT_LOG_DEBUG("Processed request (RequestId: %v, FailedAttempts: 0)",
            request->RequestId);
    }
};

YT_FLOW_DEFINE_COMPUTATION(TRequestProcessor);

// [END request_processor]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN state_keeper]
class TStateKeeper
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr output) override
    {
        const auto ysonMessage = ConvertToYsonMessage(message);

        if (ysonMessage->Meta->StreamId == "event") {
            auto event = ysonMessage->As<TEventMessage>();
            auto request = New<TRequestMessage>();
            request->RequestId = RandomNumber<ui64>();
            request->Key = event->Key;
            request->Request = event->Data;
            output->AddMessage(ConvertToMessage(request));
            YT_LOG_DEBUG("Send request (RequestId: %v)", request->RequestId);
        } else if (ysonMessage->Meta->StreamId == "response") {
            auto response = ysonMessage->As<TResponseMessage>();
            YT_LOG_DEBUG("Received response (RequestId: %v)", response->RequestId);
            auto state = StateClient_.GetState(message->Key);
            i64 totalLength = state->GetColumnValue<std::optional<i64>>("total_length").value_or(0);
            totalLength += response->Length;
            TPayloadBuilder builder(state->Schema);
            builder.Set(totalLength, "total_length");
            state->Payload = builder.Finish();
        } else {
            THROW_ERROR_EXCEPTION("Unexpected stream_id")
                << TErrorAttribute("stream_id", ysonMessage->Meta->StreamId);
        }
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TStateKeeper);

// [END state_keeper]

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TEventMessage>("event");
    builder.RegisterStream<TRequestMessage>("request");
    builder.RegisterStream<TResponseMessage>("response");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
