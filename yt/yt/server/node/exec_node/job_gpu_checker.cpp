#include "job_gpu_checker.h"
#include "slot.h"

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/server/lib/exec_node/config.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////////////////

TJobGpuChecker::TJobGpuChecker(
    TJobGpuCheckerContext context,
    NLogging::TLogger logger)
    : Context_(std::move(context))
    , Logger(std::move(logger)
        .WithTag("Type: %v", Context_.GpuCheckType))
{
    YT_LOG_DEBUG("Creating job GPU checker");
}

TFuture<void> TJobGpuChecker::RunGpuCheck()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    int checkStartIndex = 0;

    switch (Context_.GpuCheckType) {
        case EGpuCheckType::Preliminary:
            checkStartIndex = Context_.CurrentStartIndex;
            break;
        case EGpuCheckType::Extra:
            checkStartIndex = Context_.CurrentStartIndex + 2;
            break;
        default:
            Y_UNREACHABLE();
    }

    RunCheck_.Fire();

    {
        YT_LOG_INFO("Verifying GPU check command");

        auto testFileCommand = New<TShellCommandConfig>();
        testFileCommand->Path = "/usr/bin/test";
        testFileCommand->Args = {"-f", Context_.GpuCheckBinaryPath};

        auto testFileResultOrError = WaitFor(
            Context_.Slot->RunSetupCommands(
                Context_.Job->GetId(),
                {testFileCommand},
                Context_.RootFS,
                Context_.CommandUser,
                /*devices*/ std::nullopt,
                /*startIndex*/ checkStartIndex));

        if (!testFileResultOrError.IsOK()) {
            auto error = TError(NExecNode::EErrorCode::GpuCheckCommandIncorrect, "Failed to verify GPU check binary")
                << TErrorAttribute("check_type", Context_.GpuCheckType)
                << TErrorAttribute("path", Context_.GpuCheckBinaryPath)
                << testFileResultOrError;

            YT_LOG_INFO(error);

            THROW_ERROR error;
        }

        YT_LOG_INFO("GPU check command successfully verified");
    }

    auto checkCommand = New<TShellCommandConfig>();
    checkCommand->Path = Context_.GpuCheckBinaryPath;
    checkCommand->Args = std::move(Context_.GpuCheckBinaryArgs);

    YT_LOG_INFO("Running GPU check commands");

    if (Context_.TestExtraGpuCheckCommandFailure) {
        YT_LOG_INFO("Testing extra GPU check command failed");

        return MakeFuture(TError("Testing extra GPU check command failed"));
    }

    return Context_.Slot->RunSetupCommands(
        Context_.Job->GetId(),
        {checkCommand},
        Context_.RootFS,
        Context_.CommandUser,
        Context_.GpuDevices,
        /*startIndex*/ checkStartIndex + 1)
        // We want to destroy checker in job thread,
        // so we pass the only reference to it in callback calling in job thread.
        .ApplyUnique(BIND(&OnGpuCheckFinished, Passed(MakeStrong(this)))
            .AsyncVia(Context_.Job->GetInvoker()))
        .ToUncancelable();
}

void TJobGpuChecker::OnGpuCheckFinished(TJobGpuCheckerPtr checker, TErrorOr<std::vector<TShellCommandOutput>>&& result)
{
    YT_ASSERT_THREAD_AFFINITY(checker->JobThread);

    const auto& Logger = checker->Logger;

    if (result.IsOK()) {
        YT_VERIFY(std::ssize(result.Value()) == 1);
        const auto& gpuCheckOutput = result.Value().front();

        YT_LOG_INFO("GPU check command completed (Stdout: %Qv, Stderr: %Qv)",
            gpuCheckOutput.Stdout,
            gpuCheckOutput.Stderr);
    } else {
        YT_LOG_INFO(result, "GPU check command failed");
    }

    checker->FinishCheck_.Fire();

    result.ThrowOnError();
}

TJobGpuChecker::~TJobGpuChecker()
{
    YT_LOG_DEBUG("Destroying job GPU checker");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
