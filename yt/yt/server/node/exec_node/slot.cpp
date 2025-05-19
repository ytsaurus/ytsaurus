#include "slot.h"

#include "bootstrap.h"
#include "chunk_cache.h"
#include "private.h"
#include "job_environment.h"
#include "slot_location.h"
#include "slot_manager.h"
#include "volume_manager.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/proc.h>

#include <util/folder/dirut.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

static NLogging::TLogger SlotLogger("Slot");

using namespace NBus;
using namespace NConcurrency;
using namespace NContainers;
using namespace NDataNode;
using namespace NTools;
using namespace NYTree;

using NNet::TIP6Address;

////////////////////////////////////////////////////////////////////////////////

template<typename F>
concept CCallableReturningFuture = requires(F f) {
    { f() } -> CFuture;
};

////////////////////////////////////////////////////////////////////////////////

class TUserSlot
    : public IUserSlot
{
public:
    TUserSlot(
        TSlotManager* slotManager,
        TSlotLocationPtr location,
        IJobEnvironmentPtr environment,
        IVolumeManagerPtr volumeManager,
        NExecNode::IBootstrap* bootstrap,
        const TString& nodeTag,
        ESlotType slotType,
        double requestedCpu,
        NScheduler::NProto::TDiskRequest diskRequest,
        const std::optional<TNumaNodeInfo>& numaNodeAffinity)
        : JobEnvironment_(std::move(environment))
        , Location_(std::move(location))
        , VolumeManager_(std::move(volumeManager))
        , Bootstrap_(std::move(bootstrap))
        , SlotGuard_(slotManager->AcquireSlot(slotType, requestedCpu, numaNodeAffinity))
        , SlotIndex_(SlotGuard_->GetSlotIndex())
        , NodeTag_(nodeTag)
        , JobProxyUnixDomainSocketPath_(GetJobProxyUnixDomainSocketPath())
        , NumaNodeAffinity_(numaNodeAffinity)
        , Logger(SlotLogger().WithTag("SlotIndex: %v", SlotIndex_))
    {
        Location_->IncreaseSessionCount();
        if (diskRequest.disk_space() > 0) {
            Location_->AcquireDiskSpace(SlotIndex_, diskRequest.disk_space());
        }
    }

    ~TUserSlot()
    {
        YT_LOG_FATAL_IF(IsEnabled_.load(), "UserSlot was not manually disabled before destruction");
    }

    void ResetState() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        bool wasEnabled = IsEnabled_.exchange(false);

        YT_LOG_FATAL_UNLESS(wasEnabled, "Attempt to disable already disabled UserSlot");
        Location_->ReleaseDiskSpace(SlotIndex_);
        Location_->DecreaseSessionCount();
        SlotGuard_.reset();
    }

    TFuture<void> CleanProcesses() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!CleanProcessesFuture_) {
            // First kill all processes that may hold open handles to slot directories.
            CleanProcessesFuture_ = BIND(&IJobEnvironment::CleanProcesses, JobEnvironment_)
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(SlotIndex_, SlotGuard_->GetSlotType());
            return CleanProcessesFuture_;
        } else {
            return CleanProcessesFuture_.Apply(BIND(&IJobEnvironment::CleanProcesses, JobEnvironment_, SlotIndex_, SlotGuard_->GetSlotType())
                .AsyncVia(Bootstrap_->GetJobInvoker()));
        }
    }

    void CleanSandbox() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        WaitFor(Location_->CleanSandboxes(
            SlotIndex_))
            .ThrowOnError();
    }

    void CancelPreparation() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        PreparationCanceled_ = true;
    }

    void Prepare() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        PreparationCanceled_ = false;
        CleanProcessesFuture_.Reset();
    }

    TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyInternalConfigPtr config,
        TJobId jobId,
        TOperationId operationId) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "RunJobProxy",
            // Job proxy preparation is uncancelable, otherwise we might try to kill
            // a never-started job proxy process.
            /*uncancelable*/ true,
            [&] {
                if (auto delay = config->JobTestingOptions->DelayBeforeRunJobProxy) {
                    YT_LOG_DEBUG("Testing delay before run job proxy");
                    TDelayedExecutor::WaitForDuration(*delay);
                }

                YT_LOG_DEBUG("Start making job proxy config (JobId: %v)", jobId);

                {
                    auto error = WaitFor(Location_->MakeConfig(SlotIndex_, ConvertToNode(config)));
                    THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to create job proxy config");
                }

                YT_LOG_DEBUG("Finish making job proxy config (JobId: %v)", jobId);

                auto result = JobEnvironment_->RunJobProxy(
                    config,
                    SlotGuard_->GetSlotType(),
                    SlotIndex_,
                    Location_->GetSlotPath(SlotIndex_),
                    jobId,
                    operationId,
                    NumaNodeAffinity_);

                if (auto delay = config->JobTestingOptions->DelayAfterRunJobProxy) {
                    YT_LOG_DEBUG("Testing delay after run job proxy");
                    TDelayedExecutor::WaitForDuration(*delay);
                }

                return result;
            });
    }

    TFuture<void> MakeLink(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkName,
        bool executable) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "MakeLink",
            /*uncancelable*/ false,
            [&] {
                return Location_->MakeSandboxLink(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    targetPath,
                    linkName,
                    executable);
            });
    }

    TFuture<void> MakeSandboxBind(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& bindPath,
        bool executable) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "MakeBind",
            /*uncancelable*/ false,
            [&] {
                return Location_->MakeSandboxBind(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    targetPath,
                    bindPath,
                    executable);
            });
    }

    TFuture<void> MakeCopy(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TFile& destinationFile,
        const NDataNode::TChunkLocationPtr& sourceLocation) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "MakeCopy",
            /*uncancelable*/ false,
            [&] {
                return Location_->MakeSandboxCopy(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    sourcePath,
                    destinationFile,
                    sourceLocation);
            });
    }

    TFuture<void> MakeFile(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TFile& destinationFile) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "MakeFile",
            /*uncancelable*/ false,
            [&] {
                return Location_->MakeSandboxFile(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    producer,
                    destinationFile);
            });
    }

    bool IsLayerCached(const TArtifactKey& artifactKey) const override
    {
        VerifyEnabled();

        return VolumeManager_->IsLayerCached(artifactKey);
    }

    TFuture<IVolumePtr> PrepareRootVolume(
        const std::vector<TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions,
        const TUserSandboxOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!VolumeManager_) {
            return MakeFuture<IVolumePtr>(TError("Porto layers and custom root FS are not supported"));
        }

        return RunPreparationAction(
            /*actionName*/ "PrepareRootVolume",
            /*uncancelable*/ false,
            [&] {
                return VolumeManager_->PrepareVolume(layers, downloadOptions, options);
            });
    }

    TFuture<IVolumePtr> PrepareGpuCheckVolume(
        const std::vector<TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!VolumeManager_) {
            return MakeFuture<IVolumePtr>(TError("Porto layers and custom root FS are not supported"));
        }

        return RunPreparationAction(
            /*actionName*/ "PrepareGpuCheckVolume",
            /*uncancelable*/ false,
            [&] {
                return VolumeManager_->PrepareVolume(layers, downloadOptions, TUserSandboxOptions{});
            });
    }

    int GetSlotIndex() const override
    {
        VerifyEnabled();

        return SlotIndex_;
    }

    TDiskStatistics GetDiskStatistics() const override
    {
        VerifyEnabled();

        return Location_->GetDiskStatistics(SlotIndex_);
    }

    TString GetSlotPath() const override
    {
        VerifyEnabled();

        return Location_->GetSlotPath(SlotIndex_);
    }

    TString GetSandboxPath(ESandboxKind sandbox) const override
    {
        VerifyEnabled();

        return Location_->GetSandboxPath(SlotIndex_, sandbox);
    }

    TString GetMediumName() const override
    {
        VerifyEnabled();

        return Location_->GetMediumName();
    }

    TBusServerConfigPtr GetBusServerConfig() const override
    {
        VerifyEnabled();

        return TBusServerConfig::CreateUds(JobProxyUnixDomainSocketPath_);
    }

    TBusClientConfigPtr GetBusClientConfig() const override
    {
        VerifyEnabled();

        return TBusClientConfig::CreateUds(JobProxyUnixDomainSocketPath_);
    }

    TFuture<std::vector<TString>> PrepareSandboxDirectories(
        const TUserSandboxOptions& options,
        bool ignoreQuota) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "PrepareSandboxDirectories",
            // Includes quota setting and tmpfs creation.
            /*uncancelable*/ true,
            [&] {
                return Location_->PrepareSandboxDirectories(
                    SlotIndex_,
                    options,
                    ignoreQuota);
            });
    }

    TFuture<std::vector<TShellCommandOutput>> RunPreparationCommands(
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<TDevice>>& devices,
        const std::optional<TString>& hostName,
        const std::vector<TIP6Address>& ipAddresses,
        std::string tag) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "RunPreparationCommands",
            /*uncancelable*/ true,
            [&] {
                return JobEnvironment_->RunCommands(
                    SlotIndex_,
                    SlotGuard_->GetSlotType(),
                    jobId,
                    commands,
                    rootFS,
                    user,
                    devices,
                    hostName,
                    ipAddresses,
                    std::move(tag));
            });
    }

    void OnArtifactPreparationFailed(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& artifactPath,
        const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        Location_->OnArtifactPreparationFailed(
            jobId,
            SlotIndex_,
            artifactName,
            sandboxKind,
            artifactPath,
            error);
    }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return JobEnvironment_->CreateJobWorkspaceBuilder(
            invoker,
            std::move(context),
            Location_->GetJobDirectoryManager());
    }

    int GetUserId() const override
    {
        VerifyEnabled();

        return JobEnvironment_->GetUserId(SlotIndex_);
    }

    void SetAllocationId(TAllocationId allocationId) override
    {
        VerifyEnabled();

        Logger.AddTag("AllocationId: %v", allocationId);
    }

    TString GetJobProxyUnixDomainSocketPath() const override
    {
        VerifyEnabled();

        return NFS::CombinePaths({
            Location_->GetSlotPath(SlotIndex_),
            "pipes",
            Format("%v-job-proxy-%v", NodeTag_, SlotIndex_)});
    }

private:
    const IJobEnvironmentPtr JobEnvironment_;
    const TSlotLocationPtr Location_;
    const IVolumeManagerPtr VolumeManager_;

    NExecNode::IBootstrap* const Bootstrap_;

    std::atomic<bool> IsEnabled_ = true;
    std::unique_ptr<TSlotManager::TSlotGuard> SlotGuard_;
    const int SlotIndex_;

    //! Uniquely identifies a node process on the current host.
    //! Used for unix socket name generation, to communicate between node and job proxies.
    const TString NodeTag_;

    bool PreparationCanceled_ = false;

    const TString JobProxyUnixDomainSocketPath_;

    const std::optional<TNumaNodeInfo> NumaNodeAffinity_;

    TFuture<void> CleanProcessesFuture_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    NLogging::TLogger Logger;

    void VerifyEnabled() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_FATAL_UNLESS(IsEnabled_.load(), "Accessing disabled UserSlot");
    }

    template <class TName, CCallableReturningFuture TCallback>
    auto RunPreparationAction(const TName& actionName, bool uncancelable, const TCallback& action) -> decltype(action())
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        using TTypedFuture = decltype(action());
        using TUnderlyingReturnType = typename TFutureTraits<TTypedFuture>::TUnderlying;

        if (PreparationCanceled_) {
            YT_LOG_DEBUG("Skip preparation action since preparation is canceled (ActionName: %v", actionName);

            return MakeFuture<TUnderlyingReturnType>(TError("Job preparation canceled")
                << TErrorAttribute("slot_index", SlotIndex_));
        } else {
            YT_LOG_DEBUG("Running preparation action (ActionName: %v", actionName);

            auto future = action();

            return uncancelable
                ? future.ToUncancelable()
                : future;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IUserSlotPtr CreateSlot(
    TSlotManager* slotManager,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    IVolumeManagerPtr volumeManager,
    NExecNode::IBootstrap* bootstrap,
    const TString& nodeTag,
    ESlotType slotType,
    double requestedCpu,
    NScheduler::NProto::TDiskRequest diskRequest,
    const std::optional<TNumaNodeInfo>& numaNodeAffinity)
{
    auto slot = NewWithOffloadedDtor<TUserSlot>(
        bootstrap->GetJobInvoker(),
        slotManager,
        std::move(location),
        std::move(environment),
        std::move(volumeManager),
        std::move(bootstrap),
        nodeTag,
        slotType,
        requestedCpu,
        std::move(diskRequest),
        numaNodeAffinity);

    return slot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
