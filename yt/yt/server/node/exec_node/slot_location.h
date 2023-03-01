#pragma once

#include "public.h"
#include "private.h"
#include "slot.h"

#include <yt/yt/server/node/data_node/disk_location.h>
#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/tools/public.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TSlotLocation
    : public NDataNode::TDiskLocation
{
    DEFINE_BYVAL_RO_PROPERTY(int, SessionCount);

public:
    TSlotLocation(
        TSlotLocationConfigPtr config,
        IBootstrap* bootstrap,
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
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TFile& destinationFile,
        const NDataNode::TChunkLocationPtr& sourceLocation);

    TFuture<void> MakeSandboxLink(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkPath,
        bool executable);

    TFuture<void> MakeSandboxFile(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TFile& destinationFile);

    TFuture<void> MakeConfig(int slotIndex, NYTree::INodePtr config);

    TFuture<void> CleanSandboxes(int slotIndex);

    TString GetSlotPath(int slotIndex) const;

    TDiskStatistics GetDiskStatistics(int slotIndex) const;

    TString GetMediumName() const;

    NChunkClient::TMediumDescriptor GetMediumDescriptor() const;
    void SetMediumDescriptor(const NChunkClient::TMediumDescriptor& descriptor);

    void IncreaseSessionCount();
    void DecreaseSessionCount();

    NNodeTrackerClient::NProto::TDiskLocationResources GetDiskResources() const;
    void AcquireDiskSpace(int slotIndex, i64 diskSpace);
    void ReleaseDiskSpace(int slotIndex);

    NNodeTrackerClient::NProto::TSlotLocationStatistics GetSlotLocationStatistics() const;

    void Disable(const TError& error);

    void InvokeUpdateDiskResources();

    TString GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const;

    //! nullopt in #destinationPath stands for streaming into the pipe.
    void OnArtifactPreparationFailed(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const std::optional<TString>& destinationPath,
        const TError& error);

    //! Cleans the slot directory, initializes the location and enables it.
    //! If force argument is specified then unconditionally performs initialization.
    TFuture<void> Repair(bool force);

private:
    const TSlotLocationConfigPtr Config_;
    IBootstrap* const Bootstrap_;
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

    //! This invoker is a serialized wrapper for HeavyInvoker_
    const IInvokerPtr SerializedHeavyInvoker_;

    const TDiskHealthCheckerPtr HealthChecker_;
    const NConcurrency::TPeriodicExecutorPtr DiskResourcesUpdateExecutor_;
    const NConcurrency::TPeriodicExecutorPtr SlotLocationStatisticsUpdateExecutor_;
    //! Absolute path to location.
    const TString LocationPath_;

    TAtomicObject<NChunkClient::TMediumDescriptor> MediumDescriptor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SlotsLock_);

    std::set<TString> TmpfsPaths_;
    THashSet<int> SlotsWithQuota_;
    THashMap<int, TUserSandboxOptions> SandboxOptionsPerSlot_;
    THashMap<int, TDiskStatistics> DiskStatisticsPerSlot_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DiskResourcesLock_);
    NNodeTrackerClient::NProto::TDiskLocationResources DiskResources_;
    THashMap<int, i64> ReservedDiskSpacePerSlot_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SlotLocationStatisticsLock_);
    NNodeTrackerClient::NProto::TSlotLocationStatistics SlotLocationStatistics_;

    //! If location is disabled, this error contains the reason.
    TAtomicObject<TError> Error_;

    TAtomicObject<TError> Alert_;

    void ValidateEnabled() const;

    static void ValidateNotExists(const TString& path);

    bool IsInsideTmpfs(const TString& path) const;

    void EnsureNotInUse(const TString& path) const;

    void ForceSubdirectories(const TString& filePath, const TString& sandboxPath) const;

    void UpdateDiskResources();

    void UpdateSlotLocationStatistics();

    void PopulateAlerts(std::vector<TError>* alerts);

    TString GetConfigPath(int slotIndex) const;

    //! nullopt in #destinationPath stands for streaming into the pipe.
    TFuture<void> DoMakeSandboxFile(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TCallback<void()>& callback,
        const std::optional<TString>& destinationPath,
        bool canUseLightInvoker);

    void DoInitialize();

    void DoRepair(bool force);

    std::vector<TString> DoPrepareSandboxDirectories(int slotIndex, TUserSandboxOptions options, bool sandboxInsideTmpfs);

    void BuildSlotRootDirectory(int slotIndex);

    NTools::TRootDirectoryConfigPtr CreateDefaultRootDirectoryConfig(
        int slotIndex,
        std::optional<int> uid,
        int nodeUid);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
