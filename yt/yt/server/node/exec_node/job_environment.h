#pragma once

#include "job_workspace_builder.h"
#include "private.h"

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentCapabilities
{
    bool SupportsVolumeReuse = false;
    bool SupportsGpuLayers = false;
    bool SupportsSidecarDockerImages = true;
    bool SupportsSidecarVolumeMounts = true;
    bool UsePrivilegedFilesystemAccess = true;
    bool UseJobUserForPrivateSandbox = false;
    int PrivateSandboxPermissions = 0700;
};

////////////////////////////////////////////////////////////////////////////////

struct IJobEnvironment
    : public virtual TRefCounted
{
    virtual TJobEnvironmentCapabilities GetCapabilities() const = 0;

    virtual TError Init(int slotCount, double cpuLimit, double idleCpuFraction) = 0;
    virtual TFuture<void> InitSlot(int slotIndex) = 0;
    virtual void PrepareSlotForJob(int slotIndex) = 0;

    virtual TFuture<void> RunJobProxy(
        const NJobProxy::TJobProxyInternalConfigPtr& config,
        ESlotType slotType,
        int slotIndex,
        const std::string& workingDirectory,
        TJobId jobId,
        TOperationId operationId,
        const std::optional<TNumaNodeInfo>& numaNodeAffinity) = 0;

    virtual void CleanProcesses(int slotIndex, ESlotType slotType = ESlotType::Common) = 0;

    virtual void Disable(TError error) = 0;

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const std::string& path, int locationIndex) = 0;

    //! User id for user job processes.
    virtual int GetUserId(int slotIndex) const = 0;

    virtual bool IsEnabled() const = 0;

    virtual void UpdateCpuLimit(double cpuLimit) = 0;

    virtual double GetCpuLimit(ESlotType slotType) const = 0;

    virtual i64 GetMajorPageFaultCount() const = 0;

    //! Set job environment implementation-specific fields of the config.
    virtual void EnrichJobEnvironmentConfig(int slotIndex, TNonNullPtr<NJobProxy::TJobProxyInternalConfig> jobProxyConfig) const = 0;

    virtual TFuture<std::vector<TShellCommandResult>> RunCommands(
        int slotIndex,
        ESlotType slotType,
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<NContainers::TDevice>>& devices,
        const std::optional<std::string>& hostName,
        const std::vector<NNet::TIP6Address>& ipAddresses,
        std::string tag,
        bool throwOnFailedCommand) = 0;

    virtual TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) = 0;

    virtual IUserSlotPtr CreateUserSlot(TUserSlotContext context) = 0;

    virtual TJobFSSecretaryPtr CreateJobFSSecretary(NLogging::TLogger logger) = 0;

    virtual IVolumeManagerPtr CreateVolumeManager(
        const std::vector<TSlotLocationConfigPtr>& locations) = 0;

    virtual void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& oldConfig,
        const TSlotManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(
    NJobProxy::TJobEnvironmentConfig config,
    TNonNullPtr<IBootstrap> bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
