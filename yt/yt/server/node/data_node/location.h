#pragma once

#include <yt/yt/orm/library/query/heavy/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/server/lib/node/chunk_location.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_ptr.h>
#include <yt/yt/core/misc/fair_share_hierarchical_queue.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <atomic>
#include <map>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

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

    TEnumIndexedArray<EIODirection, TEnumIndexedArray<EIOCategory, NProfiling::TCounter>> CompletedIOSize;

    TEnumIndexedArray<EIODirection, TEnumIndexedArray<EIOCategory, std::atomic<i64>>> UsedMemory;
    TEnumIndexedArray<EIODirection, TEnumIndexedArray<EIOCategory, std::atomic<i64>>> LegacyUsedMemory;

    NProfiling::TCounter ThrottledReplicationReads;
    NProfiling::TCounter ThrottledProbingReads;
    NProfiling::TCounter ThrottledReads;
    std::atomic<NProfiling::TCpuInstant> LastReadThrottleTime{};

    void ReportThrottledReplicationRead();
    void ReportThrottledProbingRead();
    void ReportThrottledRead();

    NProfiling::TCounter ThrottledProbingWrites;
    NProfiling::TCounter ThrottledWrites;
    std::atomic<NProfiling::TCpuInstant> LastWriteThrottleTime{};

    void ReportThrottledProbingWrite();
    void ReportThrottledWrite();

    NProfiling::TEventTimer PutBlocksWallTime;
    NProfiling::TEventTimer BlobChunkMetaReadTime;

    NProfiling::TEventTimer BlobChunkWriterOpenTime;
    NProfiling::TEventTimer BlobChunkWriterAbortTime;
    NProfiling::TEventTimer BlobChunkWriterCloseTime;

    TEnumIndexedArray<EWorkloadCategory, NProfiling::TSummary> BlobBlockReadSize;

    TEnumIndexedArray<EWorkloadCategory, NProfiling::TEventTimer> BlobBlockReadTime;
    NProfiling::TCounter BlobBlockReadBytes;
    NProfiling::TCounter BlobBlockReadCount;

    TEnumIndexedArray<EWorkloadCategory, NProfiling::TEventTimer> BlobBlockReadLatencies;
    TEnumIndexedArray<EWorkloadCategory, NProfiling::TEventTimer> BlobChunkMetaReadLatencies;

    NProfiling::TSummary BlobBlockWriteSize;
    NProfiling::TEventTimer BlobBlockWriteTime;
    NProfiling::TCounter BlobBlockWriteBytes;

    NProfiling::TSummary JournalBlockReadSize;
    NProfiling::TEventTimer JournalBlockReadTime;
    NProfiling::TCounter JournalBlockReadBytes;

    NProfiling::TEventTimer JournalChunkCreateTime;
    NProfiling::TEventTimer JournalChunkOpenTime;
    NProfiling::TEventTimer JournalChunkRemoveTime;

    TEnumIndexedArray<ESessionType, std::atomic<int>> SessionCount;

    NProfiling::TGauge IOWeight;
    NProfiling::TGauge UsedSpace;
    NProfiling::TGauge AvailableSpace;
    NProfiling::TGauge ChunkCount;
    NProfiling::TGauge TrashChunkCount;
    NProfiling::TGauge TrashSpace;
    NProfiling::TGauge Full;
};

DEFINE_REFCOUNTED_TYPE(TLocationPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

class TLocationMemoryGuard
{
public:
    TLocationMemoryGuard() = default;
    TLocationMemoryGuard(TLocationMemoryGuard&& other);
    ~TLocationMemoryGuard();

    void Release();

    i64 GetSize() const;
    bool GetUseLegacyUsedMemory() const;
    TChunkLocationPtr GetOwner() const;

    void IncreaseSize(i64 delta);
    void DecreaseSize(i64 delta);

    TLocationMemoryGuard& operator=(TLocationMemoryGuard&& other);

    explicit operator bool() const;

private:
    friend class TChunkLocation;

    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    TLocationMemoryGuard(
        TMemoryUsageTrackerGuard memoryGuard,
        bool useLegacyUsedMemory,
        EIODirection direction,
        EIOCategory category,
        i64 size,
        TChunkLocationPtr owner);

    void MoveFrom(TLocationMemoryGuard&& other);

    TMemoryUsageTrackerGuard MemoryGuard_;
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    bool UseLegacyUsedMemory_ = false;
    EIODirection Direction_;
    EIOCategory Category_;
    i64 Size_ = 0;
    TChunkLocationPtr Owner_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkLocation
    : public NNode::TChunkLocationBase
{
public:
    TChunkLocation(
        NNode::ELocationType type,
        TString id,
        TChunkLocationConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TChunkStorePtr chunkStore,
        TChunkContextPtr chunkContext,
        IChunkStoreHostPtr chunkStoreHost);

    //! Updates the runtime configuration.
    void Reconfigure(TChunkLocationConfigPtr config);

    //! Return the maximum number of bytes in the gap between two adjacent read locations
    //! in order to join them together during read coalescing.
    i64 GetCoalescedReadMaxGapSize() const;

    //! Sets medium descriptor.
    //! #onInitialize indicates whether this method called before any data node heartbeat or on heartbeat response.
    void UpdateMediumDescriptor(
        const NChunkClient::TMediumDescriptorPtr& mediumDescriptor,
        bool onInitialize);

    //! Returns the medium name.
    std::string GetMediumName() const;

    //! Returns the medium descriptor.
    NChunkClient::TMediumDescriptorPtr GetMediumDescriptor() const;

    //! Returns various performance counters.
    TLocationPerformanceCounters& GetPerformanceCounters();

    //! Returns the IO weight of the location.
    double GetIOWeight() const;

    //! Does the node need to tell the master about this location.
    bool CanPublish() const;

    //! This method can be called either manually from the rpc method, or automatically when the check detects a recovered empty disk.
    //! To resurrect, we have to scan and register existing chunks of location.
    bool Resurrect();

    //! Returns the memory tracking for pending reads.
    const IMemoryUsageTrackerPtr& GetReadMemoryTracker() const;

    //! Returns the memory tracking for pending writes.
    const IMemoryUsageTrackerPtr& GetWriteMemoryTracker() const;

    //! Returns the max number of used memory across workloads.
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    i64 GetMaxUsedMemory(
        bool useLegacyUsedMemory,
        EIODirection direction) const;

    //! Returns the number of used memory.
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    i64 GetUsedMemory(
        bool useLegacyUsedMemory,
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor) const;

    //! Returns total amount of used memory in given #direction.
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    i64 GetUsedMemory(
        bool useLegacyUsedMemory,
        EIODirection direction) const;

    //! Acquires a lock memory for the given number of bytes to be read or written.
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    TLocationMemoryGuard AcquireLocationMemory(
        bool useLegacyUsedMemory,
        TMemoryUsageTrackerGuard memoryGuard,
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor,
        i64 delta);

    //! Acquires a lock memory for the given number of bytes to be read or written if possible.
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    TErrorOr<TLocationMemoryGuard> TryAcquireLocationMemory(
        bool useLegacyUsedMemory,
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor,
        i64 delta);

    //! Increases number of bytes done for disk IO.
    void IncreaseCompletedIOSize(
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor,
        i64 delta);

    //! Returns the incoming bandwidth throttler for a given #descriptor.
    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const;

    //! Returns the outcoming bandwidth throttler for a given #descriptor.
    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    //! Returns |true| if reads were throttled (within some recent time interval).
    bool IsReadThrottling() const;

    //! Returns |true| if writes were throttled (within some recent time interval).
    bool IsWriteThrottling() const;

    struct TDiskThrottlingResult
    {
        bool Enabled;
        bool MemoryOvercommit;
        i64 QueueSize;
        TError Error;
    };

    //! Returns whether reads must be throttled
    //! and the total number of bytes to read from disk including those accounted by out throttler.
    TDiskThrottlingResult CheckReadThrottling(
        const TWorkloadDescriptor& workloadDescriptor,
        bool isProbing = false,
        bool isReplication = false) const;

    //! Reports throttled read.
    void ReportThrottledReplicationRead() const;
    void ReportThrottledProbingRead() const;
    void ReportThrottledRead() const;

    //! Returns whether writes must be throttled.
    void ReportThrottledProbingWrite() const;
    TDiskThrottlingResult CheckWriteThrottling(
        const TWorkloadDescriptor& workloadDescriptor,
        bool blocksWindowShifted,
        bool withProbing) const;
    TDiskThrottlingResult CheckWriteThrottling(
        TChunkId sessionId,
        const TWorkloadDescriptor& workloadDescriptor,
        bool blocksWindowShifted,
        bool withProbing) const;

    //! Reports throttled write.
    void ReportThrottledWrite() const;

    //! If the tracked memory is close to the limit, new sessions will not be started.
    //! This method returns memory limit fraction.
    double GetMemoryLimitFractionForStartingNewSessions() const;

    const TChunkStorePtr& GetChunkStore() const;

    std::optional<TDuration> GetDelayBeforeBlobSessionBlockFree() const;

    double GetFairShareWorkloadCategoryWeight(EWorkloadCategory category) const;

    //! Push supplier to the queue.
    void PushProbePutBlocksRequestSupplier(const TProbePutBlocksRequestSupplierPtr& supplier);

    //! Try to acquire memory for top requests.
    void CheckProbePutBlocksRequests();

    //! Calc sum of all requested memory passeed to AcquireProbePutBlocks.
    i64 GetRequestedMemory() const;

    //! Returns size of requests queue.
    i64 GetRequestedQueueSize() const;

protected:
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const TChunkStorePtr ChunkStore_;
    const TChunkContextPtr ChunkContext_;
    const IChunkStoreHostPtr ChunkStoreHost_;

    i64 GetReadThrottlingLimit() const;
    i64 GetWriteThrottlingLimit() const;

private:
    friend class TLocationMemoryGuard;
    friend class TPendingIOGuard;
    friend class TLockedChunkGuard;

    TAtomicPtr<TChunkLocationConfig, /*EnableAcquireHazard*/ true> RuntimeConfig_;

    TAtomicIntrusivePtr<NOrm::NQuery::IExpressionEvaluator> IOWeightEvaluator_;

    TLocationPerformanceCountersPtr PerformanceCounters_;

    // TODO(vvshlyaga): Change to fair share queue.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ProbePutBlocksRequestsLock_);
    std::deque<TProbePutBlocksRequestSupplierPtr> ProbePutBlocksRequests_;
    THashSet<TSessionId> ProbePutBlocksSessionIds_;

    const IMemoryUsageTrackerPtr ReadMemoryTracker_;
    const IMemoryUsageTrackerPtr WriteMemoryTracker_;

    TChunkLocationUuid Uuid_;
    TChunkLocationIndex Index_ = NNodeTrackerClient::InvalidChunkLocationIndex;

    TAtomicIntrusivePtr<NChunkClient::TMediumDescriptor> MediumDescriptor_;
    NProfiling::TGauge MediumFlag_;

    TEnumIndexedArray<EChunkLocationThrottlerKind, NConcurrency::IReconfigurableThroughputThrottlerPtr> ReconfigurableThrottlers_;
    TEnumIndexedArray<EChunkLocationThrottlerKind, NConcurrency::IThroughputThrottlerPtr> Throttlers_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedInThrottler_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedOutThrottler_;

    bool EnableUncategorizedThrottler_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr ReconfigurableUncategorizedThrottler_;
    NConcurrency::IThroughputThrottlerPtr UncategorizedThrottler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LockedChunksLock_);
    THashSet<TChunkId> LockedChunkIds_;

    static EIOCategory ToIOCategory(const TWorkloadDescriptor& workloadDescriptor);

    bool ShouldAlwaysThrottle() const;

    THazardPtr<TChunkLocationConfig> GetRuntimeConfig() const;

    void DoCheckProbePutBlocksRequests();
    bool ContainsProbePutBlocksRequestSupplier(const TProbePutBlocksRequestSupplierPtr& supplier) const;

    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    void IncreaseUsedMemory(bool useLegacyUsedMemory, EIODirection direction, EIOCategory category, i64 delta);
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    void DecreaseUsedMemory(bool useLegacyUsedMemory, EIODirection direction, EIOCategory category, i64 delta);
    // TODO(vvshlyaga): Remove flag useLegacyUsedMemory after rolling writer with probing on all nodes.
    void UpdateUsedMemory(bool useLegacyUsedMemory, EIODirection direction, EIOCategory category, i64 delta);

    void UpdateIOWeightEvaluator(const std::optional<std::string>& formula);
    TErrorOr<double> EvaluateIOWeight(const NOrm::NQuery::IExpressionEvaluatorPtr& evaluator) const;

    void UpdateMediumTag();

    NNode::TBriefChunkLocationConfig GetBriefConfig() const;

    TChunkLocationConfigPtr GetStaticConfig() const;
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

    //! Returns the number of trash chunks.
    int GetTrashChunkCount() const;

    //! Returns the number of bytes used in trash.
    i64 GetTrashSpace() const;

    //! Returns various IO related statistics.
    TIOStatistics GetIOStatistics() const;

    //! Returns OK error if the location ready to accept new writes otherwise returns the error with reason.
    TError CheckWritable() const;

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
    const NConcurrency::TPeriodicExecutorPtr TrashCheckExecutor_;

    std::atomic<i64> TrashSpace_ = 0;
    std::atomic<int> TrashChunkCount_ = 0;

    class TIOStatisticsProvider;
    const TIntrusivePtr<TIOStatisticsProvider> IOStatisticsProvider_;

    TAtomicPtr<TStoreLocationConfig> RuntimeConfig_;

    static TJournalManagerConfigPtr BuildJournalManagerConfig(
        const TDataNodeConfigPtr& dataNodeConfig,
        const TStoreLocationConfigPtr& storeLocationConfig);

    void UpdateTrashChunkCount(int delta);
    void UpdateTrashSpace(i64 size);
    TString GetTrashPath() const;
    TString GetTrashChunkPath(TChunkId chunkId) const;
    void RegisterTrashChunk(TChunkId chunkId);
    void OnCheckTrash();
    void CheckTrashTtl();
    void CheckTrashWatermark();
    void RemoveTrashFiles(const TTrashChunkEntry& entry);
    void MoveChunkFilesToTrash(TChunkId chunkId);

    void RemoveLocationChunks();

    bool IsTrashScanStopped() const;

    bool ShouldAlwaysThrottle() const;

    i64 GetAdditionalSpace() const override;

    std::optional<NNode::TChunkDescriptor> RepairBlobChunk(TChunkId chunkId);
    std::optional<NNode::TChunkDescriptor> RepairJournalChunk(TChunkId chunkId);
    std::optional<NNode::TChunkDescriptor> RepairChunk(TChunkId chunkId) override;

    std::vector<TString> GetChunkPartNames(TChunkId chunkId) const override;
    bool ShouldSkipFileName(const std::string& fileName) const override;

    void DoStart() override;
    std::vector<NNode::TChunkDescriptor> DoScan() override;
    void DoScanTrash();
    void DoAsyncScanTrash();
};

DEFINE_REFCOUNTED_TYPE(TStoreLocation)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TStoreLocation& location, NYT::NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
