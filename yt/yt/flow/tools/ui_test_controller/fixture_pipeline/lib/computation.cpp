#include "computation.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

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

void LinkUIScreenshotPipeline()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NUIScreenshotPipeline
