#pragma once

#include "public.h"
#include "private.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/node/data_node/disk_location.h>

#include <yt/server/node/job_agent/job.h>

#include <yt/ytlib/chunk_client/medium_directory.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/atomic_object.h>

#include <yt/core/logging/log.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlotLocation
    : public NDataNode::TDiskLocation
{
    DEFINE_BYVAL_RO_PROPERTY(int, SessionCount);

public:
    TSlotLocation(
        TSlotLocationConfigPtr config,
        NClusterNode::TBootstrap* bootstrap,
        const TString& id,
        IJobDirectoryManagerPtr jobDirectoryManager,
        bool enableTmpfs,
        int slotCount,
        std::function<int(int)> slotIndexToUserId);

    TFuture<void> Initialize();

    //! Sets up tmpfs directories and applies disk quotas.
    //! Returns list of tmpfs paths.
    TFuture<std::vector<TString>> PrepareSandboxDirectories(
        int slotIndex,
        TUserSandboxOptions options);

    TFuture<void> MakeSandboxCopy(
        int slotIndex,
        ESandboxKind kind,
        const TString& sourcePath,
        const TString& destinationName,
        bool executable);

    TFuture<void> MakeSandboxLink(
        int slotIndex,
        ESandboxKind kind,
        const TString& targetPath,
        const TString& linkName,
        bool executable);

    TFuture<void> MakeSandboxFile(
        int slotIndex,
        ESandboxKind kind,
        const std::function<void(IOutputStream*)>& producer,
        const TString& destinationName,
        bool executable);

    //! Must be called when all files are prepared.
    TFuture<void> FinalizeSandboxPreparation(int slotIndex);

    TFuture<void> MakeConfig(int slotIndex, NYTree::INodePtr config);

    TFuture<void> CleanSandboxes(int slotIndex);

    TString GetSlotPath(int slotIndex) const;

    TString GetMediumName() const;

    NChunkClient::TMediumDescriptor GetMediumDescriptor() const;
    void SetMediumDescriptor(const NChunkClient::TMediumDescriptor& descriptor);

    void IncreaseSessionCount();
    void DecreaseSessionCount();

    NNodeTrackerClient::NProto::TDiskLocationResources GetDiskResources() const;

    NNodeTrackerClient::NProto::TSlotLocationStatistics GetSlotLocationStatistics() const;

    void Disable(const TError& error);

    void InvokeUpdateDiskResources();

    TString GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const;

private:
    const TSlotLocationConfigPtr Config_;
    const NClusterNode::TBootstrap* Bootstrap_;
    const IJobDirectoryManagerPtr JobDirectoryManager_;
    const bool EnableTmpfs_;
    const int SlotCount_;

    const std::function<int(int)> SlotIndexToUserId_;

    const NConcurrency::TActionQueuePtr HeavyLocationQueue_;
    const NConcurrency::TActionQueuePtr LightLocationQueue_;

    //! This invoker is used for heavy IO actions e.g. copying file to disk.
    const IInvokerPtr HeavyInvoker_;

    //! This invoker is used for light IO actions e.g. copying file to tmpfs,
    //! creating job proxy config on disk.
    const IInvokerPtr LightInvoker_;

    const TDiskHealthCheckerPtr HealthChecker_;
    const NConcurrency::TPeriodicExecutorPtr DiskResourcesUpdateExecutor_;
    const NConcurrency::TPeriodicExecutorPtr SlotLocationStatisticsUpdateExecutor_;
    //! Absolute path to location.
    const TString LocationPath_;

    TAtomicObject<NChunkClient::TMediumDescriptor> MediumDescriptor_;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, SlotsLock_);

    std::set<TString> TmpfsPaths_;
    THashSet<int> SlotsWithQuota_;
    THashMap<int, std::optional<i64>> OccupiedSlotToDiskLimit_;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, DiskResourcesLock_);
    NNodeTrackerClient::NProto::TDiskLocationResources DiskResources_;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, SlotLocationStatisticsLock_);
    NNodeTrackerClient::NProto::TSlotLocationStatistics SlotLocationStatistics_;

    //! If location is disabled, this error contains the reason.
    TAtomicObject<TError> Error_;

    void ValidateEnabled() const;

    static void ValidateNotExists(const TString& path);

    bool IsInsideTmpfs(const TString& path) const;

    void EnsureNotInUse(const TString& path) const;

    void ForceSubdirectories(const TString& filePath, const TString& sandboxPath) const;

    void UpdateDiskResources();

    void UpdateSlotLocationStatistics();

    TString GetConfigPath(int slotIndex) const;

    TFuture<void> DoMakeSandboxFile(
        int slotIndex,
        ESandboxKind kind,
        const std::function<void(const TString& destinationPath)>& callback,
        const TString& destinationName,
        bool canUseLightInvoker);

    void CreateSandboxDirectories(int slotIndex);

    void ChownChmod(
        const TString& path,
        int userId,
        int permissions);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
