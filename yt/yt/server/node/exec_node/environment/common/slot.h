#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/preparation_options.h>
#include <yt/yt/server/node/exec_node/slot.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <atomic>
#include <memory>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TUserSlotContext
{
    TNonNullPtr<TSlotManager> SlotManager;
    TSlotLocationPtr Location;
    IJobEnvironmentPtr Environment;
    IVolumeManagerPtr VolumeManager;
    TNonNullPtr<IBootstrap> Bootstrap;
    std::string NodeTag;
    ESlotType SlotType = ESlotType::Common;
    NClusterNode::TCpu RequestedCpu;
    NScheduler::NProto::TDeprecatedDiskRequest DiskRequest;
    std::optional<TNumaNodeInfo> NumaNodeAffinity;
};

////////////////////////////////////////////////////////////////////////////////

template <typename F>
concept CCallableReturningFuture = requires(F f) {
    { f() } -> CFuture;
};

class TUserSlotBase
    : public IUserSlot
{
public:
    ~TUserSlotBase() override;

    void ResetState() override;

    TFuture<void> CleanProcesses() override;

    void CleanJobWorkspaceResources(const TJobFSSecretary& fsSecretary) override;

    void CleanAllocationWorkspace() override;

    void CleanSandbox() override;

    void CancelPreparation() override;

    void Prepare() override;

    TFuture<void> BuildSlotRootDirectory() override;

    TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyInternalConfigPtr config,
        TJobId jobId,
        TOperationId operationId) override;

    TFuture<void> MakeLink(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& targetPath,
        const std::string& linkName,
        bool executable) override;

    TFuture<void> MakeFileForSandboxBind(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& targetPath,
        const std::string& bindPath,
        bool executable) override;

    TFuture<void> MakeCopy(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& sourcePath,
        const TFile& destinationFile,
        const TCacheLocationPtr& sourceLocation) override;

    TFuture<void> MakeFile(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TFile& destinationFile) override;

    int GetSlotIndex() const override;

    TDiskStatistics GetDiskStatistics() const override;

    std::string GetSlotPath() const override;

    std::string GetMediumName() const override;

    NBus::NTcp::TBusServerConfigPtr GetBusServerConfig() const override;

    NBus::NTcp::TBusClientConfigPtr GetBusClientConfig() const override;

    NBus::NTcp::TBusServerConfigPtr GetPrivateBusServerConfig() const override;

    NBus::NTcp::TBusClientConfigPtr GetPrivateBusClientConfig() const override;

    NRpc::NGrpc::TServerConfigPtr GetGrpcServerConfig() const override;

    TFuture<void> PrepareSandboxDirectories(
        const TUserSandboxOptions& options) override;

    TFuture<std::vector<TShellCommandResult>> RunPreparationCommands(
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<NContainers::TDevice>>& devices,
        const std::optional<std::string>& hostName,
        const std::vector<NNet::TIP6Address>& ipAddresses,
        std::string tag,
        bool throwOnFailedCommand) override;

    void OnArtifactPreparationFailed(
        TJobId jobId,
        const std::string& artifactName,
        ESandboxKind sandboxKind,
        const std::string& artifactPath,
        const TError& error) override;

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context) override;

    int GetUserId() const override;

    void SetAllocationId(TAllocationId allocationId) override;

    std::string GetJobProxyUnixDomainSocketPath() const override;

    std::string GetJobProxyHttpUnixDomainSocketPath() const override;

    std::string GetSandboxPath(ESandboxKind sandboxKind, const IVolumePtr& rootVolume) const override;

    TFuture<void> ValidateRootFS(const IVolumePtr& rootVolume) const override;

    void ValidateEnabled() const override;

protected:
    const IJobEnvironmentPtr JobEnvironment_;
    const TSlotLocationPtr Location_;
    const IVolumeManagerPtr VolumeManager_;

    const TNonNullPtr<NExecNode::IBootstrap> Bootstrap_;

    std::atomic<bool> IsEnabled_ = true;
    std::unique_ptr<TSlotManager::TSlotGuard> SlotGuard_;
    const int SlotIndex_;

    //! Uniquely identifies a node process on the current host.
    //! Used for unix socket name generation, to communicate between node and job proxies.
    const std::string NodeTag_;

    bool PreparationCanceled_ = false;

    const std::string JobProxyUnixDomainSocketPath_;

    const std::optional<TNumaNodeInfo> NumaNodeAffinity_;

    TFuture<void> JobProxyPreparationFuture_ = OKFuture;
    TFuture<void> CleanProcessesFuture_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    NLogging::TLogger Logger;

    explicit TUserSlotBase(TUserSlotContext context);

    TString GetRootPath(const IVolumePtr& rootVolume) const;

    TVolumePreparationContext MakeVolumePreparationContext() const;

    void VerifyEnabled() const;

    // prepareAction is invoked synchronously and may capture caller arguments by reference.
    TFuture<std::vector<TVolumeResultPtr>> DoPrepareNonRootVolumes(
        const IVolumePtr& rootVolume,
        const std::vector<TBaseVolumeParamsPtr>& volumeParams,
        const std::vector<TVolumeMountPtr>& volumeMounts,
        const TCallback<TFuture<std::vector<TVolumeResultPtr>>(const std::string&)>& prepareAction);

    template <class TName, CCallableReturningFuture TCallback>
    auto RunPreparationAction(const TName& actionName, bool uncancelable, const TCallback& action) -> decltype(action());

private:
    std::string GetJobProxyGrpcUnixDomainSocketPath() const;

    std::string GetJobProxyPrivateUnixDomainSocketPath() const;
};

DEFINE_REFCOUNTED_TYPE(TUserSlotBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

#define SLOT_INL_H_
#include "slot-inl.h"
#undef SLOT_INL_H_
