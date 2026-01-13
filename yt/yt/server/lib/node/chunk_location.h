#pragma once

#include "config.h"
#include "chunk.h"
#include "disk_location.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/fair_share_hierarchical_queue.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELocationType,
    (Store)
    (Cache)
);

////////////////////////////////////////////////////////////////////////////////

class TLocationFairShareSlot
    : public TRefCounted
{
public:
    TLocationFairShareSlot(
        TFairShareHierarchicalSlotQueuePtr<std::string> queue,
        TFairShareHierarchicalSlotQueueSlotPtr<std::string> slot);

    TFairShareHierarchicalSlotQueueSlotPtr<std::string> GetSlot() const;

    ~TLocationFairShareSlot();

private:
    TFairShareHierarchicalSlotQueuePtr<std::string> Queue_;
    TFairShareHierarchicalSlotQueueSlotPtr<std::string> Slot_;

    void MoveFrom(TLocationFairShareSlot&& other);
};

DEFINE_REFCOUNTED_TYPE(TLocationFairShareSlot)

////////////////////////////////////////////////////////////////////////////////

struct TBriefChunkLocationConfig
{
    bool AbortOnLocationDisabled;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkLocationBase
    : public NNode::TDiskLocation
{
public:
    TChunkLocationBase(
        ELocationType type,
        TString id,
        TChunkLocationConfigBasePtr config,
        TCallback<TBriefChunkLocationConfig()> getBriefConfigCallback,
        NObjectClient::TCellId cellId,
        const TFairShareHierarchicalSchedulerPtr<std::string>& fairShareHierarchicalScheduler,
        const NIO::IHugePageManagerPtr& hugePageManager,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler);

    //! Returns the type.
    ELocationType GetType() const;

    //! Returns the universally unique id.
    TChunkLocationUuid GetUuid() const;

    //! Returns the universally unique index.
    TChunkLocationIndex GetIndex() const;

    //! Sets the index of location.
    //! Verifies that index has not been changed if it was set already.
    void SetIndex(TChunkLocationIndex index);

    //! Returns the disk family
    const TString& GetDiskFamily() const;

    //! Returns the IO Engine.
    const NIO::IIOEnginePtr& GetIOEngine() const;

    //! Updates the runtime configuration.
    void Reconfigure(TChunkLocationConfigBasePtr config);

    const NProfiling::TProfiler& GetProfiler() const;

    //! Returns the root path of the location.
    const TString& GetPath() const;

    //! Returns the maximum number of bytes the chunks assigned to this location
    //! are allowed to use.
    i64 GetQuota() const;

    //! Returns an invoker for various auxiliarly IO activities.
    const IInvokerPtr& GetAuxPoolInvoker();

    //! Scan the location directory removing orphaned files and returning the list of found chunks.
    /*!
     *  If the scan fails, the location becomes disabled and an empty list is returned.
     */
    std::vector<TChunkDescriptor> Scan();

    //! Create cell id and uuid files if they don't exist.
    void InitializeIds();

    //! Prepares the location to accept new writes.
    /*!
     *  Must be called when all locations are scanned and all existing chunks are registered.
     *  On failure, acts similarly to Scan.
     */
    void Start();

    //! Subscribe callback on disk health check.
    void SubscribeDiskCheckFailed(const TCallback<void(const TError&)> callback);

    //! Updates #UsedSpace and #AvailableSpace.
    void UpdateUsedSpace(i64 size);

    //! Returns the number of bytes used at the location.
    /*!
     *  \note
     *  This may exceed #GetQuota.
     */
    i64 GetUsedSpace() const;

    //! Updates #AvailableSpace with a system call and returns the result.
    //! Never throws.
    i64 GetAvailableSpace() const;

    //! Changes the number of currently active sessions of a given #type by a given #delta.
    void UpdateSessionCount(ESessionType type, int delta);

    //! Changes the number of chunks by a given delta.
    void UpdateChunkCount(int delta);

    //! Returns the number of currently active sessions of a given #type.
    int GetSessionCount(ESessionType type) const;

    //! Returns the number of currently active sessions of any type.
    int GetSessionCount() const;

    //! Returns the number of chunks.
    int GetChunkCount() const;

    //! Returns a full path for a primary chunk file.
    TString GetChunkPath(TChunkId chunkId) const;

    //! Removes a chunk permanently or moves it to the trash (if available).
    virtual void RemoveChunkFiles(TChunkId chunkId, bool force);

    //! Location disk is OK.
    bool IsLocationDiskOK() const;

    //! Enable alert about location disk failing.
    void MarkLocationDiskFailed();

    void MarkLocationDiskWaitingReplacement();

    //! Returns |true| if location is sick.
    bool IsSick() const;

    //! Returns limit on the maximum memory used of probe put blocks.
    i64 GetLegacyWriteMemoryLimit() const;

    //! Returns limit on the maximum memory used of location reads.
    i64 GetReadMemoryLimit() const;

    //! Returns limit on the maximum memory used of location writes.
    i64 GetWriteMemoryLimit() const;

    //! Returns limit on the maximum memory used of location writes and reads.
    i64 GetTotalMemoryLimit() const;

    //! Returns limit on the maximum count of location write sessions.
    i64 GetSessionCountLimit() const;

    //! If location does not contain files corresponding to given #chunkId, acquires the lock
    //! and returns a non-null guard. Otherwise, returns a null guard.
    [[nodiscard]]
    TLockedChunkGuard TryLockChunk(TChunkId chunkId);

    //! While chunk is locked, it cannot be initialized twice.
    //! To resurrect location, all chunks are unlocked, because they need to be created anew.
    void UnlockChunk(TChunkId chunkId);

    //! Marks the location as disabled by attempting to create a lock file and marking assigned chunks
    //! as unavailable.
    virtual bool ScheduleDisable(const TError& reason) = 0;

    //! Wraps a given #callback with try/catch block that intercepts all exceptions
    //! and calls #Disable when one happens.
    template <class T>
    TCallback<T()> DisableOnError(const TCallback<T()> callback);

    TError GetLocationDisableError() const;

    //! Try changing location status to disabled. For this location disk must be active and test can run without I/O errors.
    bool OnDiskRepaired();

    TErrorOr<TLocationFairShareSlotPtr> AddFairShareQueueSlot(
        i64 size,
        std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources,
        std::vector<TFairShareHierarchyLevel<std::string>> levels);

    NIO::IIOEngineWorkloadModelPtr GetIOEngineModel() const;

    //! Destroy location on disk.
    bool StartDestroy();

    //! Mark location as destroyed, called after location disk recovering.
    bool FinishDestroy(
        bool destroyResult,
        const TError& reason);

protected:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    NObjectClient::TCellId CellId_;

    NProfiling::TProfiler Profiler_;

    NThreading::TAtomicObject<TError> LocationDisabledAlert_;
    NThreading::TAtomicObject<TError> LocationDiskFailedAlert_;

    NServer::TDiskHealthCheckerPtr HealthChecker_;

    mutable std::atomic<i64> AvailableSpace_ = 0;
    std::atomic<i64> UsedSpace_ = 0;
    TEnumIndexedArray<ESessionType, std::atomic<int>> PerTypeSessionCount_;
    std::atomic<int> ChunkCount_ = 0;

    NIO::IDynamicIOEnginePtr DynamicIOEngine_;
    NIO::IIOEngineWorkloadModelPtr IOEngineModel_;
    NIO::IIOEnginePtr IOEngine_;

    static TString GetRelativeChunkPath(TChunkId chunkId);
    static void ForceHashDirectories(const TString& rootPath);

    virtual bool ShouldSkipFileName(const std::string& fileName) const;

    virtual void DoStart();
    virtual std::vector<TChunkDescriptor> DoScan();

    void UnlockChunkLocks();

    void RemoveChunkFilesPermanently(TChunkId chunkId);

    TFuture<void> SynchronizeActions();
    void CreateDisableLockFile(const TError& reason);
    void ResetLocationStatistic();

    THazardPtr<TChunkLocationConfigBase> GetRuntimeConfig() const;
    TChunkLocationConfigBasePtr GetStaticConfig() const;

    void UpdateMediumTag(const std::string& mediumName);

    void PopulateAlerts(std::vector<TError>* alerts);

    //! Marks location as crashed during initialization.
    void Crash(const TError& reason);

private:
    friend class TLocationMemoryGuard;
    friend class TPendingIOGuard;
    friend class TLockedChunkGuard;

    const ELocationType Type_;
    const TChunkLocationConfigBasePtr StaticConfig_;

    TAtomicPtr<TChunkLocationConfigBase, /*EnableAcquireHazard*/ true> RuntimeConfig_;

    TCallback<TBriefChunkLocationConfig()> GetBriefConfigCallback_;

    NProfiling::TDynamicTagPtr MediumTag_;

    TChunkLocationUuid Uuid_;
    TChunkLocationIndex Index_ = NNodeTrackerClient::InvalidChunkLocationIndex;

    TFairShareHierarchicalSlotQueuePtr<std::string> IOFairShareQueue_;
    NIO::IHugePageManagerPtr HugePageManager_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LockedChunksLock_);
    THashSet<TChunkId> LockedChunkIds_;

    void ValidateWritable();
    void InitializeCellId();
    void InitializeUuid();

    void OnHealthCheckFailed(const TError& error);
    void MarkUninitializedLocationDisabled(const TError& error);

    virtual i64 GetAdditionalSpace() const;

    virtual std::optional<TChunkDescriptor> RepairChunk(TChunkId chunkId) = 0;
    virtual std::vector<TString> GetChunkPartNames(TChunkId chunkId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkLocationBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode

#define CHUNK_LOCATION_INL_H_
#include "chunk_location-inl.h"
#undef CHUNK_LOCATION_INL_H_
