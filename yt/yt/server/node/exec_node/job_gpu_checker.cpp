#include "job_gpu_checker.h"
#include "slot.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/range_helpers.h>

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
        .WithTag("Type: %v", Context_.Type))
{
    YT_LOG_DEBUG("Creating job GPU checker");
}

TFuture<void> TJobGpuChecker::RunGpuCheck()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    std::string tag;
    switch (Context_.Type) {
        case EGpuCheckType::Preliminary:
            tag = "prelim";
            break;
        case EGpuCheckType::Extra:
            tag = "extra";
            break;
    }

    RunCheck_.Fire();

    {
        YT_LOG_INFO("Verifying GPU check command");

        auto testFileCommand = New<TShellCommandConfig>();
        testFileCommand->Path = "/usr/bin/test";
        testFileCommand->Args = {"-f", Context_.Options.BinaryPath};

        auto testFileResultOrError = WaitFor(
            Context_.Slot->RunPreparationCommands(
                Context_.Job->GetId(),
                {testFileCommand},
                Context_.RootFS,
                Context_.CommandUser,
                /*devices*/ std::nullopt,
                /*hostName*/ std::nullopt,
                /*ipAddresses*/ {},
                tag + "_test"));

        if (!testFileResultOrError.IsOK()) {
            auto error = TError(NExecNode::EErrorCode::GpuCheckCommandPreparationFailed, "Failed to verify GPU check binary")
                << TErrorAttribute("check_type", Context_.Type)
                << TErrorAttribute("path", Context_.Options.BinaryPath)
                << std::move(testFileResultOrError);

            YT_LOG_INFO(error);

            THROW_ERROR error;
        }

        YT_LOG_INFO("GPU check command successfully verified");
    }

    // Setup actions should be performed only once (in preliminary GPU check).
    if (Context_.Type == EGpuCheckType::Preliminary && !Context_.Options.SetupCommands.empty()) {
        YT_LOG_INFO("Run GPU check setup commands");

        auto resultOrError = WaitFor(
            Context_.Slot->RunPreparationCommands(
                Context_.Job->GetId(),
                Context_.Options.SetupCommands,
                Context_.RootFS,
                Context_.CommandUser,
                /*devices*/ std::nullopt,
                /*hostName*/ std::nullopt,
                /*ipAddresses*/ {},
                tag + "_setup"));

        if (!resultOrError.IsOK()) {
            auto error = TError(NExecNode::EErrorCode::GpuCheckCommandPreparationFailed, "Failed to run setup commands for GPU check")
                << TErrorAttribute("check_type", Context_.Type)
                << std::move(resultOrError);

            YT_LOG_INFO(error);

            THROW_ERROR error;
        }

        YT_LOG_INFO("GPU check setup commands successfully executed");
    }

    auto checkCommand = New<TShellCommandConfig>();
    checkCommand->Path = Context_.Options.BinaryPath;
    checkCommand->Args = std::move(Context_.Options.BinaryArgs);
    checkCommand->EnvironmentVariables = std::move(Context_.Options.Environment);

    checkCommand->EnvironmentVariables.emplace("YT_GPU_CHECK_TYPE", FormatEnum(Context_.Type));

    if (Context_.Options.NetworkAttributes) {
        checkCommand->EnvironmentVariables.emplace("YT_NETWORK_PROJECT_ID", ToString(Context_.Options.NetworkAttributes->ProjectId));
        for (const auto& networkAddress : Context_.Options.NetworkAttributes->Addresses) {
            checkCommand->EnvironmentVariables.emplace(
                Format("YT_IP_ADDRESS_%v", to_upper(networkAddress->Name)),
                ToString(networkAddress->Address));
        }
    }

    if (Context_.Options.InfinibandCluster) {
        checkCommand->EnvironmentVariables.emplace("YT_INFINIBAND_CLUSTER", *Context_.Options.InfinibandCluster);
    }

    YT_LOG_INFO("Running GPU check commands");

    if (Context_.TestExtraGpuCheckCommandFailure) {
        YT_LOG_INFO("Testing extra GPU check command failed");

        return MakeFuture(TError("Testing extra GPU check command failed"));
    }

    // TODO(ignat): it is not a preparation command in case of extra GPU check.
    // This place requires refactoring.
    return Context_.Slot->RunPreparationCommands(
        Context_.Job->GetId(),
        {checkCommand},
        Context_.RootFS,
        Context_.CommandUser,
        Context_.Options.Devices,
        Context_.Options.NetworkAttributes
            ? std::make_optional(Context_.Options.NetworkAttributes->HostName)
            : std::nullopt,
        Context_.Options.NetworkAttributes
            ? TransformRangeTo<std::vector<NNet::TIP6Address>>(
                Context_.Options.NetworkAttributes->Addresses,
                [] (const auto& networkAddress) { return networkAddress->Address; })
            : std::vector<NNet::TIP6Address>{},
        std::move(tag))
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
