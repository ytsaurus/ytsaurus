#pragma once

#include <yt/yt/server/node/exec_node/job_environment.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/library/process/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/system/atomic_object.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TProcessJobEnvironmentBase
    : public IJobEnvironment
{
public:
    TProcessJobEnvironmentBase(
        NJobProxy::TJobEnvironmentConfig config,
        TNonNullPtr<IBootstrap> bootstrap);

    TError Init(int slotCount, double cpuLimit, double idleCpuFraction) override;

    TFuture<void> InitSlot(int slotIndex) override;

    void PrepareSlotForJob(int slotIndex) override;

    TFuture<void> RunJobProxy(
        const NJobProxy::TJobProxyInternalConfigPtr& config,
        ESlotType slotType,
        int slotIndex,
        const std::string& workingDirectory,
        TJobId jobId,
        TOperationId operationId,
        const std::optional<TNumaNodeInfo>& numaNodeAffinity) override;

    IUserSlotPtr CreateUserSlot(TUserSlotContext context) override;

    TJobFSSecretaryPtr CreateJobFSSecretary(NLogging::TLogger logger) override;

    bool IsEnabled() const override;

    void UpdateCpuLimit(double cpuLimit) override;

    double GetCpuLimit(ESlotType slotType) const override;

    i64 GetMajorPageFaultCount() const override;

    void EnrichJobEnvironmentConfig(int slotIndex, TNonNullPtr<NJobProxy::TJobProxyInternalConfig> jobProxyConfig) const override;

    TFuture<std::vector<TShellCommandResult>> RunCommands(
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
        bool throwOnFailedCommand) override;

    void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& oldConfig,
        const TSlotManagerDynamicConfigPtr& newConfig) override;

    void Disable(TError error) override;

protected:
    struct TJobProxyProcess
    {
        TProcessBasePtr Process;
        TFuture<void> Result;
    };

    struct TProcessCreationState
        : public TRefCounted
    {
        // Read lock means processes are being created.
        // Write lock means further process creation is being disabled.
        NConcurrency::TAsyncReaderWriterLock Lock;
        bool CreationDisabled = false;
    };

    using TProcessCreationStatePtr = TIntrusivePtr<TProcessCreationState>;

    const NJobProxy::TJobEnvironmentConfig Config_;
    const TNonNullPtr<IBootstrap> Bootstrap_;

    const NConcurrency::TActionQueuePtr ActionQueue_ = New<NConcurrency::TActionQueue>("JobEnvironment");

    THashMap<int, TJobProxyProcess> JobProxyProcesses_;
    THashMap<int, TProcessCreationStatePtr> ProcessCreationStates_;

    TFuture<void> JobProxyResult_;

    std::atomic<bool> Enabled_ = true;

    TAtomicObject<TError> Alert_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual void DoInit(int slotCount, double cpuLimit, double idleCpuFraction);

    void ValidateEnabled() const;

    void EnsureJobProxyFinished(int slotIndex, bool kill);

    static TFuture<TIntrusivePtr<NConcurrency::TAsyncLockReaderGuard>> AcquireProcessCreationGuard(
        const TProcessCreationStatePtr& state);

    virtual void AddArguments(TProcessBasePtr process, int slotIndex);

private:
    void PopulateAlerts(TNonNullPtr<std::vector<TError>> alerts);

    virtual TProcessBasePtr CreateJobProxyProcess(
        const NJobProxy::TJobProxyInternalConfigPtr& config,
        int slotIndex,
        ESlotType slotType,
        TJobId jobId);

    virtual void UpdateSlotCpuSet(int slotIndex, ESlotType slotType, TStringBuf cpuSet);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
