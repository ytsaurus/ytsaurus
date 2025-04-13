#pragma once

#include "chunk_cache.h"
#include "job_workspace_builder.h"

#include <yt/yt/server/node/data_node/artifact.h>

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

    //! Sets up quotas and tmpfs.
    //! Returns tmpfs paths if any.
    virtual TFuture<std::vector<TString>> PrepareSandboxDirectories(
        const TUserSandboxOptions& options,
        bool ignoreQuota = false) = 0;

    virtual TFuture<void> MakeLink(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkPath,
        bool executable) = 0;

    virtual TFuture<void> MakeSandboxBind(
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
        const NDataNode::TChunkLocationPtr& sourceLocation) = 0;

    virtual TFuture<void> MakeFile(
        TJobId jobId,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TFile& destinationFile) = 0;

    virtual bool IsLayerCached(const NDataNode::TArtifactKey& artifactKey) const = 0;

    virtual TFuture<IVolumePtr> PrepareRootVolume(
        const std::vector<NDataNode::TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions,
        const TUserSandboxOptions& options) = 0;

    virtual TFuture<IVolumePtr> PrepareGpuCheckVolume(
        const std::vector<NDataNode::TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions) = 0;

    virtual NBus::TBusServerConfigPtr GetBusServerConfig() const = 0;
    virtual NBus::TBusClientConfigPtr GetBusClientConfig() const = 0;

    virtual int GetSlotIndex() const = 0;

    virtual int GetUserId() const = 0;

    virtual TDiskStatistics GetDiskStatistics() const = 0;

    virtual TString GetSlotPath() const = 0;

    virtual TString GetSandboxPath(ESandboxKind sandbox) const = 0;

    virtual TString GetMediumName() const = 0;

    virtual TString GetJobProxyUnixDomainSocketPath() const = 0;

    virtual TFuture<std::vector<TShellCommandOutput>> RunPreparationCommands(
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<NContainers::TDevice>>& devices,
        std::string tag) = 0;

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
    double requestedCpu,
    NScheduler::NProto::TDiskRequest diskRequest,
    const std::optional<TNumaNodeInfo>& numaNodeAffinity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
