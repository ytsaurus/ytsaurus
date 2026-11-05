#include "job_environment.h"

#include "job_workspace_builder.h"

#include <yt/yt/server/node/exec_node/environment/common/job_directory_manager.h>
#include <yt/yt/server/node/exec_node/environment/common/job_environment_base.h>
#include <yt/yt/server/node/exec_node/environment/common/volume_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/library/process/process.h>

#include <util/system/user.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

using namespace NJobProxy;

using NServer::JobProxyProgramName;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TSimpleJobEnvironment(
        TJobEnvironmentConfig config,
        TNonNullPtr<IBootstrap> bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
    { }

    TJobEnvironmentCapabilities GetCapabilities() const override
    {
        return {.UsePrivilegedFilesystemAccess = false};
    }

    void CleanProcesses(int slotIndex, ESlotType slotType) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Start clean processes")
            .With("SlotIndex", slotIndex);

        try {
            EnsureJobProxyFinished(slotIndex, true);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes")
                .With("slot_index", slotIndex)
                .With("slot_type", slotType)
                .With(ex);
            Disable(error);
            THROW_ERROR error;
        }

        YT_TLOG_DEBUG("Finish clean processes")
            .With("SlotIndex", slotIndex);
    }

    int GetUserId(int /*slotIndex*/) const override
    {
        return ::getuid();
    }

    IJobDirectoryManagerPtr CreateJobDirectoryManager(const std::string& path, int /*locationIndex*/) override
    {
        return CreateHostJobDirectoryManager(
            MounterThread_->GetInvoker(),
            path,
            Bootstrap_->GetConfig()->ExecNode->SlotManager->DetachedTmpfsUmount);
    }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        return CreateSimpleJobWorkspaceBuilder(
            invoker,
            std::move(context),
            directoryManager);
    }

    IVolumeManagerPtr CreateVolumeManager(const std::vector<TSlotLocationConfigPtr>& locations) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto future = CreateHostVolumeManager(
            locations,
            MounterThread_->GetInvoker(),
            Bootstrap_->GetConfig()->ExecNode->SlotManager->DetachedTmpfsUmount);

        return WaitFor(future)
            .ValueOrThrow(
                EErrorCode::SimpleVolumeManagerFailure,
                "Failed to initialize volume manager");
    }

private:
    const TActionQueuePtr MounterThread_ = New<TActionQueue>("Mounter");

    TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& /*config*/,
        int /*slotIndex*/,
        ESlotType /*slotType*/,
        TJobId /*jobId*/) override
    {
        auto process = New<TSimpleProcess>(JobProxyProgramName);
        process->CreateProcessGroup();
        return process;
    }
};

class TTestingJobEnvironment
    : public TSimpleJobEnvironment
{
public:
    TTestingJobEnvironment(
        TJobEnvironmentConfig config,
        TNonNullPtr<IBootstrap> bootstrap)
        : TSimpleJobEnvironment(config, bootstrap)
        , ConcreteConfig_(config.GetConcrete<TTestingJobEnvironmentConfig>())
    { }

    TJobEnvironmentCapabilities GetCapabilities() const override
    {
        return {};
    }

    i64 GetMajorPageFaultCount() const override
    {
        if (ConcreteConfig_->TestingJobEnvironmentScenario == ETestingJobEnvironmentScenario::IncreasingMajorPageFaultCount) {
            MajorPageFaultCount_ += 1000;
        }

        return MajorPageFaultCount_;
    }

private:
    const TTestingJobEnvironmentConfigPtr ConcreteConfig_;

    mutable i64 MajorPageFaultCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateSimpleJobEnvironment(NJobProxy::TJobEnvironmentConfig config, TNonNullPtr<IBootstrap> bootstrap)
{
    return New<TSimpleJobEnvironment>(std::move(config), bootstrap);
}

IJobEnvironmentPtr CreateTestingJobEnvironment(NJobProxy::TJobEnvironmentConfig config, TNonNullPtr<IBootstrap> bootstrap)
{
    return New<TTestingJobEnvironment>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
