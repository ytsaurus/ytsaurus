#pragma once

#include "public.h"
#include "private.h"
#include "slot.h"

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/tools/public.h>

#include <yt/yt/server/lib/node/disk_location.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TSlotLocation
    : public NNode::TDiskLocation
{
    DEFINE_BYVAL_RO_PROPERTY(int, SessionCount);

public:
    TSlotLocation(
        TSlotLocationConfigPtr config,
        IBootstrap* bootstrap,
        const TString& id,
        IJobDirectoryManagerPtr jobDirectoryManager,
        int slotCount,
        std::function<int(int)> slotIndexToUserId);

    TFuture<void> Initialize();

    //! Apply disk quotas.
    TFuture<void> PrepareSandboxDirectories(
        int slotIndex,
        TUserSandboxOptions options,
        bool ignoreQuota);

    //! Inform slot location about tmpfses to be used.
    void TakeIntoAccountTmpfsVolumes(
        int slotIndex,
        const IVolumePtr& rootVolume,
        const std::vector<TTmpfsVolumeResult>& volumeResults,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts);

    TFuture<void> MakeSandboxCopy(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TFile& destinationFile,
        const TCacheLocationPtr& sourceLocation);

    TFuture<void> MakeSandboxLink(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkPath,
        bool executable);

    //! Create file for container bind with proper ownership. We do it since
    //! porto creates bind target with root ownership if bind target does not
    //! exist. This is not what we need so we create bind target ourselves.
    TFuture<void> MakeFileForSandboxBind(
        TJobId jobId,
        int slotIndex,
        const TString& artifactName,
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& bindPath,
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

    std::string GetMediumName() const;

    NChunkClient::TMediumDescriptorPtr GetMediumDescriptor() const;
    void SetMediumDescriptor(const NChunkClient::TMediumDescriptorPtr& descriptor);

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
    TFuture<void> Repair();

    IJobDirectoryManagerPtr GetJobDirectoryManager();

    void OnDynamicConfigChanged(const TSlotManagerDynamicConfigPtr& config);

    TFuture<void> CreateSlotDirectories(const IVolumePtr& rootVolume, int userId) const;

    TFuture<void> CreateTmpfsDirectoriesInsideSandbox(
        const TString& userSandboxPath,
        const std::vector<TTmpfsVolumeParams>& volumeParams,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts) const;

    TFuture<void> ValidateRootFS(const IVolumePtr& rootVolume) const;

    void ValidateEnabled() const;

private:
    const TSlotLocationConfigPtr Config_;
    IBootstrap* const Bootstrap_;
    const TSlotManagerConfigPtr SlotManagerStaticConfig_;
    const IJobDirectoryManagerPtr JobDirectoryManager_;
    const int SlotCount_;

    const std::function<int(int)> SlotIndexToUserId_;

    const NConcurrency::TActionQueuePtr HeavyLocationQueue_;
    const NConcurrency::TActionQueuePtr LightLocationQueue_;
    const NConcurrency::TActionQueuePtr ToolLocationQueue_;

    //! This invoker is used for heavy IO actions e.g. copying file to disk.
    const IInvokerPtr HeavyInvoker_;

    //! This invoker is used for light IO actions e.g. copying file to tmpfs,
    //! creating job proxy config on disk.
    const IInvokerPtr LightInvoker_;

    //! This invoker is used for invoking tools in a child process.
    const IInvokerPtr ToolInvoker_;

    const NServer::TDiskHealthCheckerPtr HealthChecker_;
    const NConcurrency::TPeriodicExecutorPtr DiskResourcesUpdateExecutor_;
    const NConcurrency::TPeriodicExecutorPtr SlotLocationStatisticsUpdateExecutor_;
    //! Absolute path to location.
    const TString LocationPath_;

    TAtomicIntrusivePtr<NChunkClient::TMediumDescriptor> MediumDescriptor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SlotsLock_);

    class TSandboxTmpfsData
    {
    public:
        bool IsInsideTmpfs(const TString& path, const NLogging::TLogger& Logger) const;
        void AddSandboxPath(TString&& sandboxPath);
        void AddTmpfsPath(TString&& tmpfsPath);

    private:
        std::optional<TString> TryGetPathRelativeToSandbox(const TString& path) const;

        std::set<TString> SandboxPaths_;
        std::set<TString> TmpfsPaths_;
    };

    THashMap<int, TSandboxTmpfsData> SandboxTmpfsData_;
    THashSet<int> SlotsWithQuota_;
    THashMap<int, TUserSandboxOptions> SandboxOptionsPerSlot_;
    THashMap<int, TDiskStatistics> DiskStatisticsPerSlot_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DiskResourcesLock_);
    NNodeTrackerClient::NProto::TDiskLocationResources DiskResources_;
    THashMap<int, i64> ReservedDiskSpacePerSlot_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SlotLocationStatisticsLock_);
    NNodeTrackerClient::NProto::TSlotLocationStatistics SlotLocationStatistics_;

    //! If location is disabled, this error contains the reason.
    NThreading::TAtomicObject<TError> Error_;

    NThreading::TAtomicObject<TError> Alert_;

    NProfiling::TBufferedProducerPtr MakeCopyMetricBuffer_ = New<NProfiling::TBufferedProducer>();

    static void ValidateNotExists(const TString& path);

    bool IsInsideTmpfs(int slotIndex, const TString& path) const;

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

    void DoRepair();

    void DoPrepareSandboxDirectories(
        int slotIndex,
        TUserSandboxOptions options,
        bool ignoreQuota,
        bool sandboxInsideTmpfs);

    void BuildSlotRootDirectory(int slotIndex);

    NTools::TRootDirectoryConfigPtr CreateDefaultRootDirectoryConfig(
        int slotIndex,
        std::optional<int> uid,
        int nodeUid);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
