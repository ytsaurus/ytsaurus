#pragma once

#include "disk_location.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/profiling/profiler.h>

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

    void ThrottleRead();

    NProfiling::TCounter ThrottledWrites;
    std::atomic<NProfiling::TCpuInstant> LastWriteThrottleTime{};

    void ThrottleWrite();

    NProfiling::TEventTimer PutBlocksWallTime;
    NProfiling::TEventTimer BlobChunkMetaReadTime;

    NProfiling::TEventTimer BlobChunkWriterOpenTime;
    NProfiling::TEventTimer BlobChunkWriterAbortTime;
    NProfiling::TEventTimer BlobChunkWriterCloseTime;

    NProfiling::TSummary BlobBlockReadSize;

    NProfiling::TEventTimer BlobBlockReadTime;
    NProfiling::TCounter BlobBlockReadBytes;

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
    NProfiling::TGauge Full;
};

DEFINE_REFCOUNTED_TYPE(TLocationPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

class TLocation
    : public TDiskLocation
{
public:
    //! Raised when location becomes disabled.
    // NB: This signal can be raised in different threads.
    DEFINE_SIGNAL(void(), Disabled);

public:
    TLocation(
        ELocationType type,
        const TString& id,
        TStoreLocationConfigBasePtr config,
        NClusterNode::IBootstrapBase* bootstrap);

    //! Returns the type.
    ELocationType GetType() const;

    //! Returns the universally unique id.
    TLocationUuid GetUuid() const;

    //! Returns the disk family
    const TString& GetDiskFamily() const;

    //! Returns the IO Engine.
    const NIO::IIOEnginePtr& GetIOEngine() const;

    //! Returns whether direct IO is enabled for all chunks.
    bool IsDirectIOEnabled() const;

    //! Returns the medium name.
    TString GetMediumName() const;

    //! Sets medium name and reconfigures medium descriptors.
    //! Returns |true| if location medium was changed.
    bool UpdateMediumName(const TString& newMediumName);

    //! Returns the medium descriptor.
    const NChunkClient::TMediumDescriptor& GetMediumDescriptor() const;

    const NProfiling::TProfiler& GetProfiler() const;

    //! Returns various performance counters.
    TLocationPerformanceCounters& GetPerformanceCounters();

    //! Returns the root path of the location.
    const TString& GetPath() const;

    //! Returns the maximum number of bytes the chunks assigned to this location
    //! are allowed to use.
    i64 GetQuota() const;

    //! Return the maximum number of bytes in the gap between two adjacent read locations
    //! in order to join them together during read coalescing.
    i64 GetCoalescedReadMaxGapSize() const;

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

    //! Marks the location as disabled by attempting to create a lock file and marking assinged chunks
    //! as unavailable.
    void Disable(const TError& reason);

    //! Wraps a given #callback with try/catch block that intercepts all exceptions
    //! and calls #Disable when one happens.
    template <class T>
    TCallback<T()> DisableOnError(const TCallback<T()> callback);

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

    //! Returns the number of bytes pending for disk IO.
    i64 GetPendingIOSize(
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor);

    //! Returns the maximum number of bytes pending for disk IO in given #direction.
    i64 GetMaxPendingIOSize(EIODirection direction);

    //! Acquires a lock for the given number of bytes to be read or written.
    TPendingIOGuard IncreasePendingIOSize(
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

    //! Permanently removes the files comprising a given chunk.
    void RemoveChunkFilesPermanently(TChunkId chunkId);

    //! Removes a chunk permanently or moves it to the trash (if available).
    virtual void RemoveChunkFiles(TChunkId chunkId, bool force);

    NConcurrency::IThroughputThrottlerPtr GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    bool IsReadThrottling();
    bool IsWriteThrottling();

    //! Returns |true| if location is sick.
    bool IsSick() const;

    //! Returns |true| if location does not contain
    //! files corresponding to given chunk id.
    bool TryLock(TChunkId chunkId, bool verbose = true);

    //! Called when all the chunk files are destroyed.
    void Unlock(TChunkId chunkId);

protected:
    NClusterNode::IBootstrapBase* const Bootstrap_;
    NProfiling::TProfiler Profiler_;

    static TString GetRelativeChunkPath(TChunkId chunkId);
    static void ForceHashDirectories(const TString& rootPath);

    virtual bool ShouldSkipFileName(const TString& fileName) const;

    virtual void DoStart();
    virtual std::vector<TChunkDescriptor> DoScan();

private:
    friend class TPendingIOGuard;

    const ELocationType Type_;
    const TStoreLocationConfigBasePtr Config_;


    TLocationUuid Uuid_;

    TAtomicObject<TError> Alert_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, MediumLock_);
    TAtomicObject<TString> MediumName_;
    std::atomic<NChunkClient::TMediumDescriptor*> CurrentMediumDescriptor_ = nullptr;
    std::vector<std::unique_ptr<NChunkClient::TMediumDescriptor>> MediumDescriptors_;

    mutable std::atomic<i64> AvailableSpace_ = 0;
    std::atomic<i64> UsedSpace_ = 0;
    TEnumIndexedVector<ESessionType, std::atomic<int>> PerTypeSessionCount_;
    std::atomic<int> ChunkCount_ = 0;

    NConcurrency::IThroughputThrottlerPtr ReplicationOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletCompactionAndPartitioningOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletLoggingOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletPreloadOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletRecoveryOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedOutThrottler_;

    NIO::IIOEnginePtr IOEngine_;

    TDiskHealthCheckerPtr HealthChecker_;

    TLocationPerformanceCountersPtr PerformanceCounters_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, LockedChunksLock_);
    THashSet<TChunkId> LockedChunks_;

    static EIOCategory ToIOCategory(const TWorkloadDescriptor& workloadDescriptor);

    void DecreasePendingIOSize(EIODirection direction, EIOCategory category, i64 delta);
    void UpdatePendingIOSize(EIODirection direction, EIOCategory category, i64 delta);

    void ValidateWritable();
    void InitializeCellId();
    void InitializeUuid();

    void OnHealthCheckFailed(const TError& error);
    void MarkAsDisabled(const TError& error);

    void PopulateAlerts(std::vector<TError>* alerts);

    virtual i64 GetAdditionalSpace() const;

    virtual std::optional<TChunkDescriptor> RepairChunk(TChunkId chunkId) = 0;

    virtual std::vector<TString> GetChunkPartNames(TChunkId chunkId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TLocation)

////////////////////////////////////////////////////////////////////////////////

class TStoreLocation
    : public TLocation
{
public:
    TStoreLocation(
        const TString& id,
        TStoreLocationConfigPtr config,
        NClusterNode::IBootstrapBase* bootstrap);

    //! Returns the location's config.
    const TStoreLocationConfigPtr& GetConfig() const;

    //! Returns Journal Manager associated with this location.
    const TJournalManagerPtr& GetJournalManager();

    //! Returns the space reserved for low watermark.
    //! Never throws.
    i64 GetLowWatermarkSpace() const;

    //! Checks whether the location is full.
    bool IsFull() const;

    //! Checks whether to location has enough space to contain file of size #size.
    bool HasEnoughSpace(i64 size) const;

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const;

    //! Removes a chunk permanently or moves it to the trash.
    virtual void RemoveChunkFiles(TChunkId chunkId, bool force) override;

private:
    const TStoreLocationConfigPtr Config_;

    const TJournalManagerPtr JournalManager_;
    const NConcurrency::TActionQueuePtr TrashCheckQueue_;

    mutable std::atomic<bool> Full_ = false;

    struct TTrashChunkEntry
    {
        TChunkId ChunkId;
        i64 DiskSpace;
    };

    YT_DECLARE_SPINLOCK(TAdaptiveLock, TrashMapSpinLock_);
    std::multimap<TInstant, TTrashChunkEntry> TrashMap_;
    std::atomic<i64> TrashDiskSpace_ = 0;
    const NConcurrency::TPeriodicExecutorPtr TrashCheckExecutor_;

    NConcurrency::IThroughputThrottlerPtr RepairInThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReplicationInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletCompactionAndPartitioningInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletLoggingInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletSnapshotInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletStoreFlushInThrottler_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedInThrottler_;

    TString GetTrashPath() const;
    TString GetTrashChunkPath(TChunkId chunkId) const;
    void RegisterTrashChunk(TChunkId chunkId);
    void OnCheckTrash();
    void CheckTrashTtl();
    void CheckTrashWatermark();
    void RemoveTrashFiles(const TTrashChunkEntry& entry);
    void MoveChunkFilesToTrash(TChunkId chunkId);

    virtual i64 GetAdditionalSpace() const override;

    std::optional<TChunkDescriptor> RepairBlobChunk(TChunkId chunkId);
    std::optional<TChunkDescriptor> RepairJournalChunk(TChunkId chunkId);
    virtual std::optional<TChunkDescriptor> RepairChunk(TChunkId chunkId) override;

    virtual std::vector<TString> GetChunkPartNames(TChunkId chunkId) const override;
    virtual bool ShouldSkipFileName(const TString& fileName) const override;

    virtual void DoStart() override;
    virtual std::vector<TChunkDescriptor> DoScan() override;

};

DEFINE_REFCOUNTED_TYPE(TStoreLocation)

////////////////////////////////////////////////////////////////////////////////

class TCacheLocation
    : public TLocation
{
public:
    TCacheLocation(
        const TString& id,
        TCacheLocationConfigPtr config,
        NClusterNode::IBootstrapBase* bootstrap);

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler() const;

private:
    const TCacheLocationConfigPtr Config_;

    const NConcurrency::IThroughputThrottlerPtr InThrottler_;

    std::optional<TChunkDescriptor> Repair(TChunkId chunkId, const TString& metaSuffix);
    virtual std::optional<TChunkDescriptor> RepairChunk(TChunkId chunkId) override;

    virtual std::vector<TString> GetChunkPartNames(TChunkId chunkId) const override;
};

DEFINE_REFCOUNTED_TYPE(TCacheLocation)

////////////////////////////////////////////////////////////////////////////////

class TPendingIOGuard
{
public:
    TPendingIOGuard() = default;
    TPendingIOGuard(TPendingIOGuard&& other) = default;
    ~TPendingIOGuard();

    void Release();

    TPendingIOGuard& operator = (TPendingIOGuard&& other) = default;

    explicit operator bool() const;
    i64 GetSize() const;

private:
    friend class TLocation;

    TPendingIOGuard(
        EIODirection direction,
        EIOCategory category,
        i64 size,
        TLocationPtr owner);

    EIODirection Direction_;
    EIOCategory Category_;
    i64 Size_ = 0;
    TLocationPtr Owner_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

#define LOCATION_INL_H_
#include "location-inl.h"
#undef LOCATION_INL_H_
