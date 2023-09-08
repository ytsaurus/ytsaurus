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

////////////////////////////////////////////////////////////////////////////////

template<typename F>
concept CCallableReturningFuture = requires(F f) {
    { f() } -> CFuture;
};

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public ISlot
{
public:
    TSlot(
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
        , Logger(SlotLogger.WithTag("SlotIndex: %v", SlotIndex_))
    {
        Location_->IncreaseSessionCount();
        if (diskRequest.disk_space() > 0) {
            Location_->AcquireDiskSpace(SlotIndex_, diskRequest.disk_space());
        }
    }

    TFuture<void> CleanProcesses() override
    {
        if (!CleanProcessesFuture_) {
            // First kill all processes that may hold open handles to slot directories.
            CleanProcessesFuture_ = BIND(&IJobEnvironment::CleanProcesses, JobEnvironment_)
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(SlotIndex_, SlotGuard_->GetSlotType());
        }

        return CleanProcessesFuture_;
    }

    void CleanSandbox() override
    {
        WaitFor(Location_->CleanSandboxes(
            SlotIndex_))
            .ThrowOnError();
        Location_->ReleaseDiskSpace(SlotIndex_);
        Location_->DecreaseSessionCount();
    }

    void CancelPreparation() override
    {
        PreparationCanceled_ = true;

        for (const auto& future : PreparationFutures_) {
            future.Cancel(TError("Job preparation canceled"));
        }
    }

    TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyConfigPtr config,
        TJobId jobId,
        TOperationId operationId) override
    {
        return RunPreparationAction([&] {
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
                SlotGuard_->GetSlotType(),
                SlotIndex_,
                Location_->GetSlotPath(SlotIndex_),
                jobId,
                operationId,
                config->StderrPath,
                NumaNodeAffinity_);

            if (auto delay = config->JobTestingOptions->DelayAfterRunJobProxy) {
                YT_LOG_DEBUG("Testing delay after run job proxy");
                TDelayedExecutor::WaitForDuration(*delay);
            }

            return result;
        },
        /*actionName*/ "RunJobProxy",
        // Job proxy preparation is uncancelable, otherwise we might try to kill
        // a never-started job proxy process.
        true);
    }

    TFuture<void> MakeLink(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkName,
        bool executable) override
    {
        return RunPreparationAction([&] {
                return Location_->MakeSandboxLink(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    targetPath,
                    linkName,
                    executable);
            },
            /*actionName*/ "MakeLink");
    }

    TFuture<void> MakeCopy(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TFile& destinationFile,
        const NDataNode::TChunkLocationPtr& sourceLocation) override
    {
        return RunPreparationAction([&] {
                return Location_->MakeSandboxCopy(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    sourcePath,
                    destinationFile,
                    sourceLocation);
            },
            /*actionName*/ "MakeCopy");
    }

    TFuture<void> MakeFile(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TFile& destinationFile) override
    {
        return RunPreparationAction([&] {
                return Location_->MakeSandboxFile(
                    jobId,
                    SlotIndex_,
                    artifactName,
                    sandboxKind,
                    producer,
                    destinationFile);
            },
            /*actionName*/ "MakeFile");
    }

    bool IsLayerCached(const TArtifactKey& artifactKey) const override
    {
        return VolumeManager_->IsLayerCached(artifactKey);
    }

    TFuture<IVolumePtr> PrepareRootVolume(
        const std::vector<TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions,
        const TUserSandboxOptions& options) override
    {
        if (!VolumeManager_) {
            return MakeFuture<IVolumePtr>(TError("Porto layers and custom root FS are not supported"));
        }
        return RunPreparationAction([&] {
                return VolumeManager_->PrepareVolume(layers, downloadOptions, options);
            },
            /*actionName*/ "PrepareRootVolume");
    }

    int GetSlotIndex() const override
    {
        return SlotIndex_;
    }

    TDiskStatistics GetDiskStatistics() const override
    {
        return Location_->GetDiskStatistics(SlotIndex_);
    }

    TString GetSlotPath() const override
    {
        return Location_->GetSlotPath(SlotIndex_);
    }

    TString GetSandboxPath(ESandboxKind sandbox) const override
    {
        return Location_->GetSandboxPath(SlotIndex_, sandbox);
    }

    TString GetMediumName() const override
    {
        return Location_->GetMediumName();
    }

    TBusServerConfigPtr GetBusServerConfig() const override
    {
        return TBusServerConfig::CreateUds(JobProxyUnixDomainSocketPath_);
    }

    TBusClientConfigPtr GetBusClientConfig() const override
    {
        return TBusClientConfig::CreateUds(JobProxyUnixDomainSocketPath_);
    }

    TFuture<std::vector<TString>> PrepareSandboxDirectories(
        const TUserSandboxOptions& options) override
    {
        return RunPreparationAction([&] {
                return Location_->PrepareSandboxDirectories(
                    SlotIndex_,
                    options);
            },
            /*actionName*/ "PrepareSandboxDirectories",
            // Includes quota setting and tmpfs creation.
            true);
    }

    TFuture<void> RunSetupCommands(
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const TString& user,
        const std::optional<std::vector<TDevice>>& devices,
        int startIndex) override
    {
        return RunPreparationAction([&] {
                return JobEnvironment_->RunSetupCommands(
                    SlotIndex_,
                    SlotGuard_->GetSlotType(),
                    jobId,
                    commands,
                    rootFS,
                    user,
                    devices,
                    startIndex);
            },
            /*actionName*/ "RunSetupCommands",
            // Setup commands are uncancelable since they are run in separate processes.
            true);
    }

    void OnArtifactPreparationFailed(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& artifactPath,
        const TError& error) override
    {
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
        return JobEnvironment_->CreateJobWorkspaceBuilder(
            invoker,
            std::move(context),
            Location_->GetJobDirectoryManager());
    }

    int GetUserId() const override
    {
        return JobEnvironment_->GetUserId(SlotIndex_);
    }

    void SetAllocationId(TAllocationId allocationId) override
    {
        Logger.AddTag("AllocationId: %v", allocationId);
    }

private:
    const IJobEnvironmentPtr JobEnvironment_;
    const TSlotLocationPtr Location_;
    const IVolumeManagerPtr VolumeManager_;

    NExecNode::IBootstrap* const Bootstrap_;

    std::unique_ptr<TSlotManager::TSlotGuard> SlotGuard_;
    const int SlotIndex_;

    //! Uniquely identifies a node process on the current host.
    //! Used for unix socket name generation, to communicate between node and job proxies.
    const TString NodeTag_;

    std::vector<TFuture<void>> PreparationFutures_;
    bool PreparationCanceled_ = false;

    const TString JobProxyUnixDomainSocketPath_;

    const std::optional<TNumaNodeInfo> NumaNodeAffinity_;

    TFuture<void> CleanProcessesFuture_;

    NLogging::TLogger Logger;

    template <CCallableReturningFuture TCallback, class TName>
    auto RunPreparationAction(const TCallback& action, const TName& actionName, bool uncancelable = false) -> decltype(action())
    {
        using TTypedFuture = decltype(action());
        using TUnderlyingReturnType = typename TFutureTraits<TTypedFuture>::TUnderlying;

        if (PreparationCanceled_) {
            YT_LOG_DEBUG("Skip preparation action since preparation is canceled (ActionName: %v", actionName);

            return MakeFuture<TUnderlyingReturnType>(TError("Job preparation canceled")
                << TErrorAttribute("slot_index", SlotIndex_));
        } else {
            YT_LOG_DEBUG("Running preparation action (ActionName: %v", actionName);

            auto future = action();
            auto preparationFuture = future.template As<void>();
            PreparationFutures_.push_back(uncancelable
                ? preparationFuture.ToUncancelable()
                : preparationFuture);
            return future;
        }
    }

    TString GetJobProxyUnixDomainSocketPath() const
    {
        return NFS::CombinePaths({
            Location_->GetSlotPath(SlotIndex_),
            "pipes",
            Format("%v-job-proxy-%v", NodeTag_, SlotIndex_)});
    }
};

////////////////////////////////////////////////////////////////////////////////

ISlotPtr CreateSlot(
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
    auto slot = New<TSlot>(
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
