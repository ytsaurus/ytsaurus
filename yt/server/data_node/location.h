#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/misc/disk_location.h>

#include <yt/ytlib/chunk_client/chunk_info.pb.h>
#include <yt/ytlib/chunk_client/medium_directory.h>
#include <yt/ytlib/chunk_client/public.h> // TODO: remove me

#include <yt/core/misc/enum.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>
#include <map>

namespace NYT {
namespace NDataNode {

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

class TLocation
    : public TDiskLocation
{
public:
    TLocation(
        ELocationType type,
        const TString& id,
        TStoreLocationConfigBasePtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Returns the type.
    ELocationType GetType() const;

    //! Returns string id.
    const TString& GetId() const;

    //! Returns the IO Engine.
    const NChunkClient::IIOEnginePtr& GetIOEngine() const;

    //! Returns the medium name.
    const TString& GetMediumName() const;

    //! Returns the medium descriptor.
    const NChunkClient::TMediumDescriptor& GetMediumDescriptor() const;

    //! Initializes the medium descriptor.
    void SetMediumDescriptor(const NChunkClient::TMediumDescriptor& descriptor);

    //! Returns the profiler tagged with location id.
    const NProfiling::TProfiler& GetProfiler() const;

    //! Returns the root path of the location.
    TString GetPath() const;

    //! Returns the maximum number of bytes the chunks assigned to this location
    //! are allowed to use.
    i64 GetQuota() const;

    //! Returns an invoker for reading chunk data.
    IPrioritizedInvokerPtr GetDataReadInvoker();

    //! Returns an invoker for reading chunk meta.
    IPrioritizedInvokerPtr GetMetaReadInvoker();

    //! Returns an invoker for writing chunks.
    IInvokerPtr GetWritePoolInvoker();

    //! Scan the location directory removing orphaned files and returning the list of found chunks.
    /*!
     *  If the scan fails, the location becomes disabled, |Disabled| signal is raised, and an empty list is returned.
     */
    std::vector<TChunkDescriptor> Scan();

    //! Prepares the location to accept new writes.
    /*!
     *  Must be called when all locations are scanned and all existing chunks are registered.
     *  On failure, acts similarly to Scan.
     */
    void Start();

    //! Marks the location as disabled by attempting to create a lock file and terminates the process.
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

    //! Returns the load factor.
    double GetLoadFactor() const;

    //! Returns the number of bytes pending for disk IO.
    i64 GetPendingIOSize(
        EIODirection direction,
        const TWorkloadDescriptor& workloadDescriptor);

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
    TString GetChunkPath(const TChunkId& chunkId) const;

    //! Permanently removes the files comprising a given chunk.
    void RemoveChunkFilesPermanently(const TChunkId& chunkId);

    //! Removes a chunk permanently or moves it to the trash (if available).
    virtual void RemoveChunkFiles(const TChunkId& chunkId, bool force);

    //! Update PutBlocks wall time profiling counter.
    void UpdatePutBlocksWallTimeCounter(NProfiling::TValue value);

    NConcurrency::IThroughputThrottlerPtr GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    void IncrementThrottledReadsCounter();
    void IncrementThrottledWritesCounter();

    bool IsReadThrottling();
    bool IsWriteThrottling();

protected:
    NCellNode::TBootstrap* const Bootstrap_;


    static TString GetRelativeChunkPath(const TChunkId& chunkId);
    static void ForceHashDirectories(const TString& rootPath);

    virtual bool ShouldSkipFileName(const TString& fileName) const;

    virtual void DoStart();
    virtual std::vector<TChunkDescriptor> DoScan();

private:
    friend class TPendingIOGuard;

    const ELocationType Type_;
    const TString Id_;
    const TStoreLocationConfigBasePtr Config_;

    NChunkClient::TMediumDescriptor MediumDescriptor_;

    mutable i64 AvailableSpace_ = 0;
    i64 UsedSpace_ = 0;
    TEnumIndexedVector<int, ESessionType> PerTypeSessionCount_;
    int ChunkCount_ = 0;

    const NConcurrency::TThreadPoolPtr DataReadThreadPool_;
    const IPrioritizedInvokerPtr DataReadInvoker_;

    const NConcurrency::TActionQueuePtr MetaReadQueue_;
    const IPrioritizedInvokerPtr MetaReadInvoker_;

    const NConcurrency::TThreadPoolPtr WriteThreadPool_;
    const IInvokerPtr WritePoolInvoker_;

    NConcurrency::IThroughputThrottlerPtr ReplicationOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletCompactionAndPartitioningOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletLoggingOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletPreloadOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletRecoveryOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedOutThrottler_;

    const NChunkClient::IIOEnginePtr IOEngine_;

    TDiskHealthCheckerPtr HealthChecker_;

    NProfiling::TProfiler Profiler_;
    //! Indexed by |(ioDirection, ioCategory)|.
    std::vector<NProfiling::TSimpleCounter> PendingIOSizeCounters_;
    std::vector<NProfiling::TSimpleCounter> CompletedIOSizeCounters_;

    NProfiling::TAggregateCounter ThrottledReadsCounter_;
    NProfiling::TAggregateCounter ThrottledWritesCounter_;

    NProfiling::TAggregateCounter PutBlocksWallTimeCounter_;

    static EIOCategory ToIOCategory(const TWorkloadDescriptor& workloadDescriptor);
    NProfiling::TSimpleCounter& GetPendingIOSizeCounter(
        EIODirection direction,
        EIOCategory category);
    NProfiling::TSimpleCounter& GetCompletedIOSizeCounter(
        EIODirection direction,
        EIOCategory category);

    void DecreasePendingIOSize(EIODirection direction, EIOCategory category, i64 delta);
    void UpdatePendingIOSize(EIODirection direction, EIOCategory category, i64 delta);

    void ValidateLockFile();
    void ValidateWritable();

    void OnHealthCheckFailed(const TError& error);
    void MarkAsDisabled(const TError& error);

    virtual i64 GetAdditionalSpace() const;

    virtual TNullable<TChunkDescriptor> RepairChunk(const TChunkId& chunkId) = 0;

    virtual std::vector<TString> GetChunkPartNames(const TChunkId& chunkId) const = 0;

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
        NCellNode::TBootstrap* bootstrap);

    //! Returns Journal Manager associated with this location.
    TJournalManagerPtr GetJournalManager();

    //! Returns the space reserved for low watermark.
    //! Never throws.
    i64 GetLowWatermarkSpace() const;

    //! Checks whether the location is full.
    bool IsFull() const;

    //! Checks whether to location has enough space to contain file of size #size
    bool HasEnoughSpace(i64 size) const;

    NConcurrency::IThroughputThrottlerPtr GetInThrottler(const TWorkloadDescriptor& descriptor) const;

    //! Removes a chunk permanently or moves it to the trash.
    virtual void RemoveChunkFiles(const TChunkId& chunkId, bool force) override;

private:
    const TStoreLocationConfigPtr Config_;

    const TJournalManagerPtr JournalManager_;
    const NConcurrency::TActionQueuePtr TrashCheckQueue_;

    mutable std::atomic<bool> Full_ = {false};

    struct TTrashChunkEntry
    {
        TChunkId ChunkId;
        i64 DiskSpace;
    };

    TSpinLock TrashMapSpinLock_;
    std::multimap<TInstant, TTrashChunkEntry> TrashMap_;
    i64 TrashDiskSpace_ = 0;
    const NConcurrency::TPeriodicExecutorPtr TrashCheckExecutor_;

    NConcurrency::IThroughputThrottlerPtr RepairInThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReplicationInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletCompactionAndPartitioningInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletLoggingInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletSnapshotInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletStoreFlushInThrottler_;
    NConcurrency::IThroughputThrottlerPtr UnlimitedInThrottler_;

    TString GetTrashPath() const;
    TString GetTrashChunkPath(const TChunkId& chunkId) const;
    void RegisterTrashChunk(const TChunkId& chunkId);
    void OnCheckTrash();
    void CheckTrashTtl();
    void CheckTrashWatermark();
    void RemoveTrashFiles(const TTrashChunkEntry& entry);
    void MoveChunkFilesToTrash(const TChunkId& chunkId);

    virtual i64 GetAdditionalSpace() const override;

    TNullable<TChunkDescriptor> RepairBlobChunk(const TChunkId& chunkId);
    TNullable<TChunkDescriptor> RepairJournalChunk(const TChunkId& chunkId);
    virtual TNullable<TChunkDescriptor> RepairChunk(const TChunkId& chunkId) override;

    virtual std::vector<TString> GetChunkPartNames(const TChunkId& chunkId) const override;
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
        NCellNode::TBootstrap* bootstrap);

    NConcurrency::IThroughputThrottlerPtr GetInThrottler() const;

private:
    const TCacheLocationConfigPtr Config_;

    NConcurrency::IThroughputThrottlerPtr InThrottler_;

    TNullable<TChunkDescriptor> Repair(const TChunkId& chunkId, const TString& metaSuffix);
    virtual TNullable<TChunkDescriptor> RepairChunk(const TChunkId& chunkId) override;

    virtual std::vector<TString> GetChunkPartNames(const TChunkId& chunkId) const override;
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

    TPendingIOGuard& operator = (TPendingIOGuard&& other);

    explicit operator bool() const;
    i64 GetSize() const;

    friend void swap(TPendingIOGuard& lhs, TPendingIOGuard& rhs);

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

} // namespace NDataNode
} // namespace NYT

#define LOCATION_INL_H_
#include "location-inl.h"
#undef LOCATION_INL_H_
