#pragma once

#include "disk_location.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/atomic_ptr.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/library/profiling/sensor.h>

#include <atomic>
#include <map>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELocationType,
    (Store)
    (Cache)
);

DEFINE_ENUM(EIODirection,
    (Read)
    (Write)
);

DEFINE_ENUM(EIOCategory,
    (Repair)
    (Batch)
    (Interactive)
    (Realtime)
);

////////////////////////////////////////////////////////////////////////////////

struct TLocationPerformanceCounters
    : public TRefCounted
{
    explicit TLocationPerformanceCounters(const NProfiling::TProfiler& profiler);

    TEnumIndexedVector<EIODirection, TEnumIndexedVector<EIOCategory, std::atomic<i64>>> PendingIOSize;
    TEnumIndexedVector<EIODirection, TEnumIndexedVector<EIOCategory, NProfiling::TCounter>> CompletedIOSize;

    NProfiling::TCounter ThrottledReads;
    std::atomic<NProfiling::TCpuInstant> LastReadThrottleTime{};

    void ReportThrottledRead();

    NProfiling::TCounter ThrottledWrites;
    std::atomic<NProfiling::TCpuInstant> LastWriteThrottleTime{};

    void ReportThrottledWrite();

    NProfiling::TEventTimer PutBlocksWallTime;
    NProfiling::TEventTimer BlobChunkMetaReadTime;

    NProfiling::TEventTimer BlobChunkWriterOpenTime;
    NProfiling::TEventTimer BlobChunkWriterAbortTime;
    NProfiling::TEventTimer BlobChunkWriterCloseTime;

    TEnumIndexedVector<EWorkloadCategory, NProfiling::TSummary> BlobBlockReadSize;

    TEnumIndexedVector<EWorkloadCategory, NProfiling::TEventTimer> BlobBlockReadTime;
    NProfiling::TCounter BlobBlockReadBytes;
    NProfiling::TCounter BlobBlockReadCount;

    TEnumIndexedVector<EWorkloadCategory, NProfiling::TEventTimer> BlobBlockReadLatencies;
    TEnumIndexedVector<EWorkloadCategory, NProfiling::TEventTimer> BlobChunkMetaReadLatencies;

    NProfiling::TSummary BlobBlockWriteSize;
    NProfiling::TEventTimer BlobBlockWriteTime;
    NProfiling::TCounter BlobBlockWriteBytes;

    NProfiling::TSummary JournalBlockReadSize;
    NProfiling::TEventTimer JournalBlockReadTime;
    NProfiling::TCounter JournalBlockReadBytes;

    NProfiling::TEventTimer JournalChunkCreateTime;
    NProfiling::TEventTimer JournalChunkOpenTime;
    NProfiling::TEventTimer JournalChunkRemoveTime;

    TEnumIndexedVector<ESessionType, std::atomic<int>> SessionCount;

    NProfiling::TGauge UsedSpace;
    NProfiling::TGauge AvailableSpace;
    NProfiling::TGauge ChunkCount;
    NProfiling::TGauge Full;
};

DEFINE_REFCOUNTED_TYPE(TLocationPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

class TPendingIOGuard
{
public:
    TPendingIOGuard() = default;
    TPendingIOGuard(TPendingIOGuard&& other);
    ~TPendingIOGuard();

    void Release();

    TPendingIOGuard& operator=(TPendingIOGuard&& other);

    explicit operator bool() const;

private:
    friend class TChunkLocation;

    TPendingIOGuard(
        TMemoryUsageTrackerGuard memoryGuard,
        EIODirection direction,
        EIOCategory category,
        i64 size,
        TChunkLocationPtr owner);

    void MoveFrom(TPendingIOGuard&& other);

    TMemoryUsageTrackerGuard MemoryGuard_;
    EIODirection Direction_;
    EIOCategory Category_;
    i64 Size_ = 0;
    TChunkLocationPtr Owner_;
};

////////////////////////////////////////////////////////////////////////////////

class TLockedChunkGuard
{
public:
    TLockedChunkGuard() = default;
    TLockedChunkGuard(TLockedChunkGuard&& other);
    ~TLockedChunkGuard();

    //! This method loses pointer to location and chunk for exclude
    //! Location::UnlockChunk call in destructor. This is necessary to preserve
    //! eternal (while the location is alive) lock on the chunk.
    void Release();

    TLockedChunkGuard& operator=(TLockedChunkGuard&& other);

    explicit operator bool() const;

private:
    friend class TChunkLocation;

    TLockedChunkGuard(TChunkLocationPtr location, TChunkId chunkId);

    void MoveFrom(TLockedChunkGuard&& other);

    TChunkLocationPtr Location_;
    TChunkId ChunkId_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkLocation
    : public TDiskLocation
{
public:
    TChunkLocation(
        ELocationType type,
        TString id,
        TChunkLocationConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TChunkStorePtr chunkStore,
        TChunkContextPtr chunkContext,
        IChunkStoreHostPtr chunkStoreHost);

    //! Returns the type.
    ELocationType GetType() const;

    //! Returns the universally unique id.
    TChunkLocationUuid GetUuid() const;

    //! Returns the disk family
    const TString& GetDiskFamily() const;

    //! Returns the IO Engine.
    const NIO::IIOEnginePtr& GetIOEngine() const;

    //! Returns the IO Engine with stats observer.
    const NIO::IIOEngineWorkloadModelPtr& GetIOEngineModel() const;

    //! Updates the runtime configuration.
    void Reconfigure(TChunkLocationConfigPtr config);

    //! Return the maximum number of bytes in the gap between two adjacent read locations
    //! in order to join them together during read coalescing.
    i64 GetCoalescedReadMaxGapSize() const;

    //! Returns the medium name.
    TString GetMediumName() const;

    //! Sets medium descriptor.
    //! #onInitialize indicates whether this method called before any data node heartbeat or on heartbeat response.
    void UpdateMediumDescriptor(
        const NChunkClient::TMediumDescriptor& mediumDescriptor,
        bool onInitialize);

    //! Returns the medium descriptor.
    NChunkClient::TMediumDescriptor GetMediumDescriptor() const;

    const NProfiling::TProfiler& GetProfiler() const;

    //! Returns various performance counters.
    TLocationPerformanceCounters& GetPerformanceCounters();

    //! Returns the root path of the location.
    const TString& GetPath() const;

    //! Returns the maximum number of bytes the chunks assigned to this location
    //! are allowed to use.
    i64 GetQuota() const;

    //! Returns the IO weight of the location.
    double GetIOWeight() const;

    //! Returns an invoker for various auxiliarly IO activities.
    const IInvokerPtr& GetAuxPoolInvoker();

    //! Scan the location directory removing orphaned files and returning the list of found chunks.
    /*!
     *  If the scan fails, the location becomes disabled and an empty list is returned.
     */
    std::vector<TChunkDescriptor> Scan();

    //! Prepares the location to accept new writes.
    /*!
     *  Must be called when all locations are scanned and all existing chunks are registered.
     *  On failure, acts similarly to Scan.
     */
    void Start();

    //! Does the node need to tell the master about this location.
    bool CanPublish() const;

    //! Try changing location status to disabled. For this location disk must be active and test can run without I/O errors.
    bool OnDiskRepaired();

    //! This method can be called either manually from the rpc method, or automatically when the check detects a recovered empty disk.
    //! To resurrect, we have to scan and register existing chunks of location.
    bool Resurrect();

    //! Destroy location on disk.
    bool StartDestroy();

    //! Mark location as destroyed, called after location disk recovering.
    bool FinishDestroy(
        bool destroyResult,
        const TError& reason);

    //! Marks location as crashed during initialization. Master must not find out about this location.
    void Crash(const TError& reason);

    //! Subscribe callback on disk health check.
    void SubscribeDiskCheckFailed(const TCallback<void(const TError&)> callback);

    //! Updates #UsedSpace and #AvailableSpace
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

    //! Returns the memory tracking for pending reads.
    const ITypedNodeMemoryTrackerPtr& GetReadMemoryTracker() const;

    //! Returns the memory tracking for pending writes.
    const ITypedNodeMemoryTrackerPtr& GetWriteMemoryTracker() const;

    //! Returns the number of bytes pending for disk IO.
    i64 GetPendingIOSize(
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor) const;

    //! Returns the maximum number of bytes pending for disk IO in given #direction.
    i64 GetMaxPendingIOSize(EIODirection direction) const;

    //! Acquires a lock for the given number of bytes to be read or written.
    TPendingIOGuard AcquirePendingIO(
        TMemoryUsageTrackerGuard memoryGuard,
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor,
        i64 delta);

    //! Increases number of bytes done for disk IO.
    void IncreaseCompletedIOSize(
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor,
        i64 delta);

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

    //! Returns the incoming bandwidth throttler for a given #descriptor.
    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const;

    //! Returns the outcoming bandwidth throttler for a given #descriptor.
    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    //! Returns |true| if reads were throttled (within some recent time interval).
    bool IsReadThrottling() const;

    //! Returns |true| if writes were throttled (within some recent time interval).
    bool IsWriteThrottling() const;

    //! Returns whether reads must be throttled
    //! and the total number of bytes to read from disk including those accounted by out throttler.
    std::tuple<bool, i64> CheckReadThrottling(
        const TWorkloadDescriptor& workloadDescriptor,
        bool incrementCounter = true) const;

    //! Reports throttled read.
    void ReportThrottledRead() const;

    //! Returns |true| if writes must currently be throttled.
    bool CheckWriteThrottling(
        const TWorkloadDescriptor& workloadDescriptor,
        bool incrementCounter = true) const;

    //! Reports throttled write.
    void ReportThrottledWrite() const;

    //! Location disk is OK.
    bool IsLocationDiskOK() const;

    //! Enable alert about location disk failing.
    void MarkLocationDiskFailed();

    //! Returns |true| if location is sick.
    bool IsSick() const;

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

    const TChunkStorePtr& GetChunkStore() const;

protected:
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const TChunkStorePtr ChunkStore_;
    const TChunkContextPtr ChunkContext_;
    const IChunkStoreHostPtr ChunkStoreHost_;

    NProfiling::TProfiler Profiler_;

    TAtomicObject<TError> LocationDisabledAlert_;
    TAtomicObject<TError> LocationDiskFailedAlert_;

    mutable std::atomic<i64> AvailableSpace_ = 0;
    std::atomic<i64> UsedSpace_ = 0;
    TEnumIndexedVector<ESessionType, std::atomic<int>> PerTypeSessionCount_;
    std::atomic<int> ChunkCount_ = 0;

    static TString GetRelativeChunkPath(TChunkId chunkId);
    static void ForceHashDirectories(const TString& rootPath);

    virtual bool ShouldSkipFileName(const TString& fileName) const;

    virtual void DoStart();
    virtual std::vector<TChunkDescriptor> DoScan();

    i64 GetReadThrottlingLimit() const;
    i64 GetWriteThrottlingLimit() const;

    void RemoveChunkFilesPermanently(TChunkId chunkId);

    TFuture<void> SynchronizeActions();
    void CreateDisableLockFile(const TError& reason);
    void ResetLocationStatistic();

private:
    friend class TPendingIOGuard;
    friend class TLockedChunkGuard;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    const ELocationType Type_;
    const TChunkLocationConfigPtr StaticConfig_;

    TLocationPerformanceCountersPtr PerformanceCounters_;

    const ITypedNodeMemoryTrackerPtr ReadMemoryTracker_;
    const ITypedNodeMemoryTrackerPtr WriteMemoryTracker_;

    TAtomicPtr<TChunkLocationConfig, /*EnableAcquireHazard*/ true> RuntimeConfig_;

    TChunkLocationUuid Uuid_;

    TAtomicObject<NChunkClient::TMediumDescriptor> MediumDescriptor_;
    NProfiling::TDynamicTagPtr MediumTag_;
    NProfiling::TGauge MediumFlag_;

    TEnumIndexedVector<EChunkLocationThrottlerKind, NConcurrency::IReconfigurableThroughputThrottlerPtr> ReconfigurableThrottlers_;
    TEnumIndexedVector<EChunkLocationThrottlerKind, NConcurrency::IThroughputThrottlerPtr> Throttlers_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedInThrottler_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedOutThrottler_;

    NIO::IDynamicIOEnginePtr DynamicIOEngine_;
    NIO::IIOEngineWorkloadModelPtr IOEngineModel_;
    NIO::IIOEnginePtr IOEngine_;

    TDiskHealthCheckerPtr HealthChecker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LockedChunksLock_);
    THashSet<TChunkId> LockedChunkIds_;

    static EIOCategory ToIOCategory(const TWorkloadDescriptor& workloadDescriptor);

    THazardPtr<TChunkLocationConfig> GetRuntimeConfig() const;

    void DecreasePendingIOSize(EIODirection direction, EIOCategory category, i64 delta);
    void UpdatePendingIOSize(EIODirection direction, EIOCategory category, i64 delta);

    void ValidateWritable();
    void InitializeCellId();
    void InitializeUuid();

    void UpdateMediumTag();

    void OnHealthCheckFailed(const TError& error);
    void MarkUninitializedLocationDisabled(const TError& error);

    void PopulateAlerts(std::vector<TError>* alerts);

    virtual i64 GetAdditionalSpace() const;

    virtual std::optional<TChunkDescriptor> RepairChunk(TChunkId chunkId) = 0;
    virtual std::vector<TString> GetChunkPartNames(TChunkId chunkId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkLocation)

////////////////////////////////////////////////////////////////////////////////

class TStoreLocation
    : public TChunkLocation
{
public:
    struct TIOStatistics
    {
        i64 FilesystemReadRate = 0;
        i64 FilesystemWriteRate = 0;

        i64 DiskReadRate = 0;
        i64 DiskWriteRate = 0;
    };

    TStoreLocation(
        TString id,
        TStoreLocationConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TChunkStorePtr chunkStore,
        TChunkContextPtr chunkContext,
        IChunkStoreHostPtr chunkStoreHost);

    ~TStoreLocation();

    //! Returns the static config.
    const TStoreLocationConfigPtr& GetStaticConfig() const;

    //! Returns the runtime config.
    TStoreLocationConfigPtr GetRuntimeConfig() const;

    //! Updates the runtime configuration.
    void Reconfigure(TStoreLocationConfigPtr config);

    //! Returns Journal Manager associated with this location.
    const IJournalManagerPtr& GetJournalManager();

    //! Returns the space reserved for low watermark.
    //! Never throws.
    i64 GetLowWatermarkSpace() const;

    //! Returns max allowed write rate by device warranty.
    //! Never throws.
    i64 GetMaxWriteRateByDwpd() const;

    //! Checks whether the location is full.
    bool IsFull() const;

    //! Checks whether to location has enough space to contain file of size #size.
    bool HasEnoughSpace(i64 size) const;

    //! Removes a chunk permanently or moves it to the trash.
    void RemoveChunkFiles(TChunkId chunkId, bool force) override;

    //! Returns various IO related statistics.
    TIOStatistics GetIOStatistics() const;

    //! Returns |true| if the location accepts new writes.
    bool IsWritable() const;

    //! Marks the location as disabled by attempting to create a lock file and marking assigned chunks
    //! as unavailable.
    bool ScheduleDisable(const TError& reason) override;

private:
    const TStoreLocationConfigPtr StaticConfig_;

    const IJournalManagerPtr JournalManager_;
    const NConcurrency::TActionQueuePtr TrashCheckQueue_;

    mutable std::atomic<bool> Full_ = false;
    mutable std::atomic<bool> WritesDisabledDueToHighPendingReadSize_ = false;

    struct TTrashChunkEntry
    {
        TChunkId ChunkId;
        i64 DiskSpace;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TrashMapSpinLock_);
    std::multimap<TInstant, TTrashChunkEntry> TrashMap_;
    std::atomic<i64> TrashDiskSpace_ = 0;
    const NConcurrency::TPeriodicExecutorPtr TrashCheckExecutor_;

    class TIOStatisticsProvider;
    const TIntrusivePtr<TIOStatisticsProvider> IOStatisticsProvider_;

    TAtomicPtr<TStoreLocationConfig> RuntimeConfig_;

    static TJournalManagerConfigPtr BuildJournalManagerConfig(
        const TDataNodeConfigPtr& dataNodeConfig,
        const TStoreLocationConfigPtr& storeLocationConfig);

    TString GetTrashPath() const;
    TString GetTrashChunkPath(TChunkId chunkId) const;
    void RegisterTrashChunk(TChunkId chunkId);
    void OnCheckTrash();
    void CheckTrashTtl();
    void CheckTrashWatermark();
    void RemoveTrashFiles(const TTrashChunkEntry& entry);
    void MoveChunkFilesToTrash(TChunkId chunkId);

    void RemoveLocationChunks();

    i64 GetAdditionalSpace() const override;

    std::optional<TChunkDescriptor> RepairBlobChunk(TChunkId chunkId);
    std::optional<TChunkDescriptor> RepairJournalChunk(TChunkId chunkId);
    std::optional<TChunkDescriptor> RepairChunk(TChunkId chunkId) override;

    std::vector<TString> GetChunkPartNames(TChunkId chunkId) const override;
    bool ShouldSkipFileName(const TString& fileName) const override;

    void DoStart() override;
    std::vector<TChunkDescriptor> DoScan() override;
};

DEFINE_REFCOUNTED_TYPE(TStoreLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

#define LOCATION_INL_H_
#include "location-inl.h"
#undef LOCATION_INL_H_
