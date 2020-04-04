#pragma once

#include "public.h"
#include "private.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/server/node/data_node/disk_location.h>

#include <yt/server/node/job_agent/job.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/fs.h>

#include <yt/core/logging/log.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlotLocation
    : public NDataNode::TDiskLocation
{
    DEFINE_BYVAL_RO_PROPERTY(int, SessionCount);

public:
    TSlotLocation(
        const TSlotLocationConfigPtr& config,
        const NCellNode::TBootstrap* bootstrap,
        const TString& id,
        const IJobDirectoryManagerPtr& jobDirectoryManager,
        bool enableTmpfs,
        int slotCount);

    //! Make ./sandbox, ./home/, ./udf and other directories.
    //! Returns list of tmpfs paths.
    TFuture<std::vector<TString>> CreateSandboxDirectories(
        int slotIndex,
        TUserSandboxOptions options,
        int userId);

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

    // Set quota, permissions, etc. Must be called when all files are prepared.
    TFuture<void> FinalizeSandboxPreparation(
        int slotIndex,
        int userId);

    TFuture<void> MakeConfig(int slotIndex, NYTree::INodePtr config);

    TFuture<void> CleanSandboxes(int slotIndex);

    TString GetSlotPath(int slotIndex) const;

    void IncreaseSessionCount();
    void DecreaseSessionCount();

    NNodeTrackerClient::NProto::TDiskLocationResources GetDiskResources() const;

    void Disable(const TError& error);

private:
    const TSlotLocationConfigPtr Config_;
    const NCellNode::TBootstrap* Bootstrap_;

    const IJobDirectoryManagerPtr JobDirectoryManager_;

    NConcurrency::TActionQueuePtr LocationQueue_;

    const bool EnableTmpfs_;
    const bool HasRootPermissions_;

    std::set<TString> TmpfsPaths_;
    std::set<int> SlotsWithQuota_;

    NConcurrency::TReaderWriterSpinLock SlotsLock_;
    THashMap<int, std::optional<i64>> OccupiedSlotToDiskLimit_;

    TDiskHealthCheckerPtr HealthChecker_;

    NNodeTrackerClient::NProto::TDiskLocationResources DiskResources_;
    NConcurrency::TReaderWriterSpinLock DiskResourcesLock_;
    NConcurrency::TPeriodicExecutorPtr DiskResourcesUpdateExecutor_;

    void ValidateEnabled() const;

    static void ValidateNotExists(const TString& path);

    bool IsInsideTmpfs(const TString& path) const;

    void EnsureNotInUse(const TString& path) const;

    void ForceSubdirectories(const TString& filePath, const TString& sandboxPath) const;

    void UpdateDiskResources();

    TString GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const;
    TString GetConfigPath(int slotIndex) const;

    TFuture<void> DoMakeSandboxFile(
        int slotIndex,
        ESandboxKind kind,
        const std::function<void(const TString& destinationPath)>& callback,
        const TString& destinationName);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
