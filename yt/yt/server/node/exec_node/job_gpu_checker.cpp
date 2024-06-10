#include "job_gpu_checker.h"
#include "slot.h"

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/server/lib/exec_node/config.h>

namespace NYT::NExecNode
{

using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////////////////

TJobGpuChecker::TJobGpuChecker(
    TJobGpuCheckerContext context,
    NLogging::TLogger logger)
    : Context_(std::move(context))
    , Logger(std::move(logger))
{
    YT_LOG_DEBUG("Creating job GPU checker (Type: %v)", Context_.GpuCheckType);
}

TFuture<void> TJobGpuChecker::RunGpuCheck()
{
    VERIFY_THREAD_AFFINITY(JobThread);

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
        YT_LOG_INFO("Verifying GPU check command (Type: %v)", Context_.GpuCheckType);

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
            YT_LOG_INFO(
                testFileResultOrError,
                "Path to %lv GPU check binary is not a file",
                Context_.GpuCheckType);

            THROW_ERROR_EXCEPTION(EErrorCode::GpuCheckCommandIncorrect, "Path to GPU check binary is not a file")
                << TErrorAttribute("path", Context_.GpuCheckBinaryPath)
                << testFileResultOrError;
        }

        YT_LOG_INFO("%v GPU check command successfully verified", Context_.GpuCheckType);
    }

    auto checkCommand = New<TShellCommandConfig>();
    checkCommand->Path = Context_.GpuCheckBinaryPath;
    checkCommand->Args = std::move(Context_.GpuCheckBinaryArgs);

    YT_LOG_INFO("Running GPU check commands (Type: %v)", Context_.GpuCheckType);

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
    VERIFY_THREAD_AFFINITY(checker->JobThread);

    const auto& Logger = checker->Logger;

    if (result.IsOK()) {
        YT_VERIFY(std::ssize(result.Value()) == 1);
        const auto& gpuCheckOutput = result.Value().front();

        YT_LOG_INFO("%v GPU check command completed (Stdout: %Qv, Stderr: %Qv)",
            checker->Context_.GpuCheckType,
            gpuCheckOutput.Stdout,
            gpuCheckOutput.Stderr);
    } else {
        YT_LOG_INFO(result, "%v GPU check command failed", checker->Context_.GpuCheckType);
    }

    checker->FinishCheck_.Fire();

    result.ThrowOnError();
}

TJobGpuChecker::~TJobGpuChecker()
{
    YT_LOG_DEBUG("Destroying job GPU checker (Type: %v)", Context_.GpuCheckType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
