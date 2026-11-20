#include "computation.h"

#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <fstream>

#include <unistd.h>

namespace NYT::NFlow::NUIScreenshotPipeline {

////////////////////////////////////////////////////////////////////////////////

struct TSlowPassthroughParameters
    : public virtual TTransformComputation::TParameters
{
    TDuration ProcessingDelay;
    std::string CommitGateReadyPath;
    std::string CommitGateReleasePath;

    REGISTER_YSON_STRUCT(TSlowPassthroughParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("processing_delay", &TThis::ProcessingDelay)
            .Default();
        registrar.Parameter("commit_gate_ready_path", &TThis::CommitGateReadyPath)
            .Default();
        registrar.Parameter("commit_gate_release_path", &TThis::CommitGateReleasePath)
            .Default();
    }
};

class TSlowPassthroughComputation
    : public TTransformComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSlowPassthroughParameters);

    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr /*context*/) override
    {
        THROW_ERROR_EXCEPTION_UNLESS(
            GetSpec()->OutputStreamIds.size() == 1,
            "Expected exactly one output stream");
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        HasInputs_ = true;
        NConcurrency::TDelayedExecutor::WaitForDuration(GetParameters()->ProcessingDelay);
        output->AddMessage(ConvertToOutputMessage(message, *GetSpec()->OutputStreamIds.begin()));
    }

    void DoSync(IRetryableTransactionPtr transaction) override
    {
        TTransformComputation::DoSync(transaction);
        if (HasInputs_ && !GetParameters()->CommitGateReadyPath.empty()) {
            std::ofstream(GetParameters()->CommitGateReadyPath).close();
            while (::access(GetParameters()->CommitGateReleasePath.c_str(), F_OK) != 0) {
                NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));
            }
        }
        HasInputs_ = false;
    }

private:
    bool HasInputs_ = false;
};

YT_FLOW_DEFINE_COMPUTATION(TSlowPassthroughComputation);

////////////////////////////////////////////////////////////////////////////////

struct TMessageCountState
    : public NYTree::TYsonStruct
{
    i64 MessageCount = 0;

    REGISTER_YSON_STRUCT(TMessageCountState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("message_count", &TThis::MessageCount)
            .Default(0);
    }
};

class TReferenceWriterFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(ReferenceStateClient_, "/reference_state");
        initContext->InitClient(CountStateClient_, "/state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto referenceState = ReferenceStateClient_.GetState(message->Key);
        TPayloadBuilder builder(referenceState->Schema);
        builder.Set(std::string("reference"), "value");
        referenceState->Payload = builder.Finish();

        ++CountStateClient_.GetState(message->Key)->MessageCount;
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> ReferenceStateClient_;
    TMutableStateKeyClient<TMessageCountState> CountStateClient_;
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TReferenceWriterFunction);

void LinkUIScreenshotPipeline()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NUIScreenshotPipeline
