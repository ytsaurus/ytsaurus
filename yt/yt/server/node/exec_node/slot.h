#pragma once

#include "artifact.h"
#include "artifact_cache.h"
#include "job_workspace_builder.h"

#include <yt/yt/library/containers/public.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/client/formats/format.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>

#include <util/stream/file.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TDiskStatistics
{
    std::optional<i64> Limit;
    i64 Usage;
};

////////////////////////////////////////////////////////////////////////////////

struct IUserSlot
    : public NClusterNode::ISlot
{
    //! Kill all possibly running processes and clean sandboxes.
    virtual TFuture<void> CleanProcesses() = 0;

    virtual void CleanSandbox() = 0;

    virtual void CancelPreparation() = 0;

    virtual void Prepare() = 0;

    virtual TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyInternalConfigPtr config,
        TJobId jobId,
        TOperationId operationId) = 0;

    //! Sets up quotas.
    virtual TFuture<void> PrepareSandboxDirectories(
        const TUserSandboxOptions& options,
        bool ignoreQuota = false) = 0;

    virtual TFuture<void> MakeLink(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkPath,
        bool executable) = 0;

    virtual TFuture<void> MakeFileForSandboxBind(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& bindPath,
        bool executable) = 0;

    virtual TFuture<void> MakeCopy(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TFile& destinationFile,
        const TCacheLocationPtr& sourceLocation) = 0;

    virtual TFuture<void> MakeFile(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TFile& destinationFile) = 0;

    virtual bool IsLayerCached(const TArtifactKey& artifactKey) const = 0;

    virtual TFuture<IVolumePtr> PrepareRootVolume(
        const std::vector<TArtifactKey>& layers,
        const TVolumePreparationOptions& options) = 0;

    virtual TFuture<IVolumePtr> PrepareGpuCheckVolume(
        const std::vector<TArtifactKey>& layers,
        const TVolumePreparationOptions& options) = 0;

    virtual TFuture<std::vector<TTmpfsVolumeResult>> PrepareTmpfsVolumes(
        const IVolumePtr& rootVolume,
        const std::vector<TTmpfsVolumeParams>& volumes,
        bool testRootFs) = 0;

    virtual TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const TString& slotPath) = 0;

    virtual TFuture<void> LinkTmpfsVolumes(
        const IVolumePtr& rootVolume,
        const std::vector<TTmpfsVolumeResult>& volumes,
        bool testRootFs) = 0;

    virtual NBus::TBusServerConfigPtr GetBusServerConfig() const = 0;
    virtual NBus::TBusClientConfigPtr GetBusClientConfig() const = 0;

    virtual NRpc::NGrpc::TServerConfigPtr GetGrpcServerConfig() const = 0;

    virtual int GetSlotIndex() const = 0;

    virtual int GetUserId() const = 0;

    virtual TDiskStatistics GetDiskStatistics() const = 0;

    virtual TString GetSlotPath() const = 0;

    virtual TString GetSandboxPath(ESandboxKind sandboxKind, const IVolumePtr& rootVolume, bool testRootFs) const = 0;

    virtual std::string GetMediumName() const = 0;

    virtual TString GetJobProxyUnixDomainSocketPath() const = 0;
    virtual std::string GetJobProxyHttpUnixDomainSocketPath() const = 0;

    virtual TFuture<std::vector<TShellCommandResult>> RunPreparationCommands(
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<NContainers::TDevice>>& devices,
        const std::optional<TString>& hostName,
        const std::vector<NNet::TIP6Address>& ipAddresses,
        std::string tag,
        bool throwOnFailedCommand) = 0;

    virtual void OnArtifactPreparationFailed(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& artifactPath,
        const TError& error) = 0;

    virtual TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context) = 0;

    //! Must be called before any action with slot.
    virtual void SetAllocationId(TAllocationId allocationId) = 0;

    virtual TFuture<void> CreateSlotDirectories(const IVolumePtr& rootVolume, int userId) const = 0;

    virtual TFuture<void> ValidateRootFS(const IVolumePtr& rootVolume) const = 0;

    virtual void ValidateEnabled() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserSlot)

////////////////////////////////////////////////////////////////////////////////

IUserSlotPtr CreateSlot(
    TSlotManager* slotManager,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    IVolumeManagerPtr volumeManager,
    NExecNode::IBootstrap* bootstrap,
    const TString& nodeTag,
    ESlotType slotType,
    NClusterNode::TCpu requestedCpu,
    NScheduler::NProto::TDeprecatedDiskRequest diskRequest,
    const std::optional<TNumaNodeInfo>& numaNodeAffinity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
