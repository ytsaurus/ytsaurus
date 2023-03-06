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

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TJobGpuChecker::TJobGpuChecker(
    const TJobGpuCheckerSettings& settings)
    : Settings_(std::move(settings))
{ }

TFuture<void> TJobGpuChecker::RunGpuCheck()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Running %lv GPU check commands", Settings_.GpuCheckType);

    int checkStartIndex = 0;

    switch (Settings_.GpuCheckType) {
        case EGpuCheckType::Preliminary:
            checkStartIndex = Settings_.CurrentStartIndex;
            break;
        case EGpuCheckType::Extra:
            checkStartIndex = Settings_.CurrentStartIndex + 2;
            break;
        default:
            Y_UNREACHABLE();
    }

    RunCheck_.Fire();

    {
        auto testFileCommand = New<TShellCommandConfig>();
        testFileCommand->Path = "/usr/bin/test";
        testFileCommand->Args = {"-f", Settings_.GpuCheckBinaryPath};

        auto testFileError = WaitFor(
            Settings_.Slot->RunSetupCommands(
                Settings_.Job->GetId(),
                {testFileCommand},
                Settings_.RootFs,
                Settings_.CommandUser,
                /*devices*/ std::nullopt,
                /*startIndex*/ checkStartIndex));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            testFileError,
            EErrorCode::GpuCheckCommandIncorrect,
            "Path to GPU check binary is not a file %v",
            Settings_.GpuCheckBinaryPath);
    }

    auto checkCommand = New<TShellCommandConfig>();
    checkCommand->Path = Settings_.GpuCheckBinaryPath;
    checkCommand->Args = std::move(Settings_.GpuCheckBinaryArgs);

    if (Settings_.TestExtraGpuCheckCommandFailure) {
        return MakeFuture(TError("Testing extra GPU check command failed"));
    }

    return Settings_.Slot->RunSetupCommands(
        Settings_.Job->GetId(),
        {checkCommand},
        Settings_.RootFs,
        Settings_.CommandUser,
        Settings_.GpuDevices,
        /*startIndex*/ checkStartIndex + 1)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] () {
            FinishCheck_.Fire();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
