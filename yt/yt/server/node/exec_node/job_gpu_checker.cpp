#include "job_gpu_checker.h"

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

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
    YT_LOG_DEBUG("Creating %lv job GPU checker", Context_.GpuCheckType);
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
        YT_LOG_INFO("Verifying %lv GPU check commands", Context_.GpuCheckType);

        auto testFileCommand = New<TShellCommandConfig>();
        testFileCommand->Path = "/usr/bin/test";
        testFileCommand->Args = {"-f", Context_.GpuCheckBinaryPath};

        auto testFileError = WaitFor(
            Context_.Slot->RunSetupCommands(
                Context_.Job->GetId(),
                {testFileCommand},
                Context_.RootFs,
                Context_.CommandUser,
                /*devices*/ std::nullopt,
                /*startIndex*/ checkStartIndex));

        if (!testFileError.IsOK()) {
            YT_LOG_INFO(
                testFileError,
                "Path to %lv GPU check binary is not a file",
                Context_.GpuCheckType);

            THROW_ERROR_EXCEPTION(EErrorCode::GpuCheckCommandIncorrect, "Path to GPU check binary is not a file")
                << TErrorAttribute("path", Context_.GpuCheckBinaryPath)
                << testFileError;
        }

        YT_LOG_INFO("%lv GPU check commands successfully verified", Context_.GpuCheckType);
    }

    auto checkCommand = New<TShellCommandConfig>();
    checkCommand->Path = Context_.GpuCheckBinaryPath;
    checkCommand->Args = std::move(Context_.GpuCheckBinaryArgs);

    YT_LOG_INFO("Running %lv GPU check commands", Context_.GpuCheckType);

    if (Context_.TestExtraGpuCheckCommandFailure) {
        YT_LOG_INFO("Testing extra GPU check command failed");

        return MakeFuture(TError("Testing extra GPU check command failed"));
    }

    return Context_.Slot->RunSetupCommands(
        Context_.Job->GetId(),
        {checkCommand},
        Context_.RootFs,
        Context_.CommandUser,
        Context_.GpuDevices,
        /*startIndex*/ checkStartIndex + 1)
        // We want to destroy checker in job thread,
        // so we pass the only reference to it in callback calling in job thread.
        .Apply(BIND(&OnGpuCheckFinished, Passed(MakeStrong(this)))
            .AsyncVia(Context_.Job->GetInvoker()));
}

void TJobGpuChecker::OnGpuCheckFinished(TJobGpuCheckerPtr checker, const TError& result)
{
    VERIFY_THREAD_AFFINITY(checker->JobThread);

    const auto& Logger = checker->Logger;

    YT_LOG_INFO(result, "%lv GPU check commands finished", checker->Context_.GpuCheckType);
    checker->FinishCheck_.Fire();

    result.ThrowOnError();
}

TJobGpuChecker::~TJobGpuChecker()
{
    YT_LOG_DEBUG("Destroying %lv job GPU checker", Context_.GpuCheckType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
