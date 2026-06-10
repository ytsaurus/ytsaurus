#include "slot.h"

#include "bootstrap.h"
#include "job_environment.h"
#include "private.h"
#include "slot_location.h"
#include "slot_manager.h"
#include "volume.h"
#include "volume_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

static NLogging::TLogger SlotLogger("Slot");

using namespace NBus;
using namespace NBus::NTcp;
using namespace NConcurrency;
using namespace NContainers;
using namespace NNode;
using namespace NScheduler;
using namespace NTools;
using namespace NYTree;

using NNet::TIP6Address;

////////////////////////////////////////////////////////////////////////////////

template <typename F>
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
        const std::string& nodeTag,
        ESlotType slotType,
        NClusterNode::TCpu requestedCpu,
        NScheduler::NProto::TDeprecatedDiskRequest diskRequest,
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

    void CleanUserImportedPortoResources(const THashSet<std::string>& preservedVolumePaths) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        YT_VERIFY(VolumeManager_);

        Location_->RemoveVolumesFromPortoPlace(SlotIndex_, VolumeManager_, preservedVolumePaths);
        Location_->RemoveLayersFromPortoPlace(SlotIndex_, VolumeManager_);
    }

    void CleanPortoPlace() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        WaitFor(Location_->CleanPortoPlace(SlotIndex_))
            .ThrowOnError();
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

                // Enrich the configuration with the environment-specific parameters.
                JobEnvironment_->EnrichJobEnvironmentConfig(SlotIndex_, config);

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
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& targetPath,
        const std::string& linkName,
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

    TFuture<void> MakeFileForSandboxBind(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& targetPath,
        const std::string& bindPath,
        bool executable) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        return RunPreparationAction(
            /*actionName*/ "MakeBind",
            /*uncancelable*/ false,
            [&] {
                return Location_->MakeFileForSandboxBind(
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
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& sourcePath,
        const TFile& destinationFile,
        const TCacheLocationPtr& sourceLocation) override
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
        const std::string& artifactName,
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

    std::vector<TFuture<TOverlayData>> PrepareLayers(
        TJobId jobId,
        const std::vector<TOverlayLayerPreparationOptions>& layerOptions,
        const TArtifactDownloadOptions& artifactDownloadOptions) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!VolumeManager_) {
            return {};
        }

        return VolumeManager_->PrepareOverlayLayers(jobId, layerOptions, artifactDownloadOptions);
    }

    TFuture<IVolumePtr> PrepareRootVolume(
        std::vector<TOverlayData> overlayDataArray,
        const TVolumePreparationOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!VolumeManager_) {
            return MakeFuture<IVolumePtr>(TError("Cannot prepare root volume without volume manager."));
        }

        return RunPreparationAction(
            /*actionName*/ "PrepareRootVolume",
            /*uncancelable*/ false,
            [&] {
                return VolumeManager_->PrepareVolume(
                    std::move(overlayDataArray),
                    options);
            });
    }

    // COMPAT(krasovav): Remove when LinkRootFS is ready
    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!VolumeManager_) {
            return MakeFuture<IVolumePtr>(TError("Cannot bind root volume without volume manager."));
        }

        return RunPreparationAction(
            /*actionName*/ "RbindRootVolume",
            /*uncancelable*/ false,
            [&] {
                return VolumeManager_->RbindRootVolume(volume, GetSlotPath());
            });
    }

    TFuture<IVolumePtr> PrepareGpuCheckVolume(
        std::vector<TOverlayData> overlayDataArray,
        const TVolumePreparationOptions& options) override
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
                return VolumeManager_->PrepareVolume(
                    std::move(overlayDataArray),
                    options);
            });
    }

    TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        TJobId jobId,
        const IVolumePtr& rootVolume,
        const std::vector<TBaseVolumeParamsPtr>& volumeParams,
        std::vector<std::vector<TOverlayData>> perVolumeOverlayData,
        const std::vector<TVolumeMountPtr>& volumeMounts,
        bool testRootFs) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        YT_LOG_DEBUG(
            "Preparing non-root volumes (Volumes: %v)",
            MakeFormattableView(
                volumeParams,
                [] (auto* builder, const TBaseVolumeParamsPtr& volume) {
                    builder->AppendFormat("{VolumeId: %v}", volume->VolumeId);
                }));

        if (!VolumeManager_) {
            auto error = TError("Failed to prepare non-root volumes since volume manager is not initialized");
            YT_LOG_WARNING(error);
            return MakeFuture<std::vector<TVolumeResultPtr>>(std::move(error));
        }

        auto userSandboxPath = GetSandboxPath(ESandboxKind::User, rootVolume, testRootFs);
        return RunPreparationAction(
            /*actionName*/ "PrepareNonRootVolumes",
            /*uncancelable*/ false,
            [&] {
                return VolumeManager_->PrepareNonRootVolumes(userSandboxPath, jobId, volumeParams, std::move(perVolumeOverlayData), volumeMounts)
                    .AsUnique()
                    .Apply(
                        BIND(
                            [
                                rootVolume,
                                volumeMounts,
                                this,
                                this_ = MakeStrong(this)
                            ] (TErrorOr<std::vector<TVolumeResultPtr>>&& volumeResultsOrError) {
                                if (!volumeResultsOrError.IsOK()) {
                                    THROW_ERROR_EXCEPTION("Failed to prepare non-root volumes: %v",
                                        volumeResultsOrError);
                                }

                                auto& volumeResults = volumeResultsOrError.Value();

                                // Inform slot location about tmpfses to be used.
                                Location_->TakeIntoAccountTmpfsVolumes(
                                    SlotIndex_,
                                    rootVolume,
                                    volumeResults,
                                    volumeMounts);
                                return std::move(volumeResults);
                    })
                    .AsyncVia(Bootstrap_->GetJobInvoker()));
            });

    }

    TFuture<void> LinkVolumes(
        const IVolumePtr& rootVolume,
        const std::vector<TVolumeResultPtr>& volumeResults,
        const std::vector<TVolumeMountPtr>& volumeMounts,
        bool testRootFs) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        VerifyEnabled();

        if (!VolumeManager_) {
            auto error = TError("Failed to link volumes since volume manager is not initialized");
            YT_LOG_WARNING(error);
            return MakeFuture<void>(std::move(error));
        }

        auto userSandboxPath = GetSandboxPath(ESandboxKind::User, rootVolume, testRootFs);

        YT_LOG_DEBUG("Linking volumes into sandbox (UserSandboxPath: %v, Volumes: %v)",
            userSandboxPath,
            MakeFormattableView(volumeResults,
                [] (auto* builder, const TVolumeResultPtr& result) {
                    builder->AppendFormat("{VolumeId: %v}",
                        result->VolumeId);
                }));

        return RunPreparationAction(
            /*actionName*/ "LinkVolumes",
            /*uncancelable*/ true,
            [userSandboxPath = std::move(userSandboxPath), volumeResults, volumeMounts, this, this_ = MakeStrong(this)] {
                return VolumeManager_->LinkVolumes(userSandboxPath, volumeResults, volumeMounts);
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

    std::string GetSlotPath() const override
    {
        VerifyEnabled();

        return Location_->GetSlotPath(SlotIndex_);
    }

    std::string GetMediumName() const override
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

    NRpc::NGrpc::TServerConfigPtr GetGrpcServerConfig() const override
    {
        VerifyEnabled();

        auto shortPath = NFS::GetRelativePath(
            Location_->GetSlotPath(SlotIndex_),
            GetJobProxyGrpcUnixDomainSocketPath());

        auto addressConfig = New<NRpc::NGrpc::TServerAddressConfig>();
        addressConfig->Address = "unix:" + shortPath;

        auto config = New<NRpc::NGrpc::TServerConfig>();
        config->Addresses.push_back(std::move(addressConfig));

        return config;
    }

    TFuture<void> PrepareSandboxDirectories(
        const TUserSandboxOptions& options,
        const std::vector<TBaseVolumeParamsPtr>& nonRootVolumeParams,
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
                    nonRootVolumeParams,
                    ignoreQuota);
            });
    }

    TFuture<std::vector<TShellCommandResult>> RunPreparationCommands(
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<TDevice>>& devices,
        const std::optional<std::string>& hostName,
        const std::vector<TIP6Address>& ipAddresses,
        std::string tag,
        bool throwOnFailedCommand) override
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
                    std::move(tag),
                    throwOnFailedCommand);
            });
    }

    void OnArtifactPreparationFailed(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& artifactPath,
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

    std::string GetJobProxyUnixDomainSocketPath() const override
    {
        VerifyEnabled();

        return NFS::CombinePaths({
            Location_->GetSlotPath(SlotIndex_),
            "pipes",
            Format("%v-job-proxy-%v", NodeTag_, SlotIndex_)});
    }

    std::string GetJobProxyHttpUnixDomainSocketPath() const override
    {
        VerifyEnabled();

        return NFS::CombinePaths({
            Location_->GetSlotPath(SlotIndex_),
            "pipes",
            Format("%v-job-proxy-http-%v", NodeTag_, SlotIndex_)});
    }

    TFuture<void> CreateSlotDirectories(const IVolumePtr& rootVolume, int userId) const override
    {
        VerifyEnabled();

        return Location_->CreateSlotDirectories(rootVolume, userId);
    }

    TFuture<void> ValidateRootFS(const IVolumePtr& rootVolume) const override
    {
        VerifyEnabled();

        return Location_->ValidateRootFS(rootVolume);
    }

    std::string GetSandboxPath(ESandboxKind sandboxKind, const IVolumePtr& rootVolume, bool testRootFs) const override
    {
        VerifyEnabled();

        switch (sandboxKind) {
            case ESandboxKind::User:
                if (rootVolume && !testRootFs) {
                    YT_VERIFY(!rootVolume->GetPath().empty());

                    // Use user sandbox within root volume.
                    return NFS::CombinePaths(
                        rootVolume->GetPath(),
                        Format("slot/%v", GetSandboxRelPath(sandboxKind)));
                }
                [[fallthrough]];
            default:
                return Location_->GetSandboxPath(
                    SlotIndex_,
                    sandboxKind);
        }
    }

    void ValidateEnabled() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        if (!IsEnabled_.load()) {
            THROW_ERROR_EXCEPTION("User slot is disabled")
                << TErrorAttribute("slot_index", SlotIndex_);
        }

        Location_->ValidateEnabled();
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
    const std::string NodeTag_;

    bool PreparationCanceled_ = false;

    const std::string JobProxyUnixDomainSocketPath_;

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
            YT_LOG_DEBUG("Skip preparation action since preparation is canceled (ActionName: %v)", actionName);

            return MakeFuture<TUnderlyingReturnType>(TError("Job preparation canceled")
                << TErrorAttribute("slot_index", SlotIndex_));
        } else {
            YT_LOG_DEBUG("Running preparation action (ActionName: %v)", actionName);

            auto future = action();

            return uncancelable
                ? future.ToUncancelable()
                : future;
        }
    }

    std::string GetJobProxyGrpcUnixDomainSocketPath() const
    {
        VerifyEnabled();

        return NFS::CombinePaths({
            Location_->GetSlotPath(SlotIndex_),
            "pipes",
            Format("%v-job-proxy-grpc-%v", NodeTag_, SlotIndex_)});
    }

};

////////////////////////////////////////////////////////////////////////////////

IUserSlotPtr CreateSlot(
    TSlotManager* slotManager,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    IVolumeManagerPtr volumeManager,
    NExecNode::IBootstrap* bootstrap,
    const std::string& nodeTag,
    ESlotType slotType,
    NClusterNode::TCpu requestedCpu,
    NScheduler::NProto::TDeprecatedDiskRequest diskRequest,
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
