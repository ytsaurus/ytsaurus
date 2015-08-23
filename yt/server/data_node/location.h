#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <core/actions/signal.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <ytlib/chunk_client/chunk_info.pb.h>

#include <server/cell_node/public.h>

#include <atomic>
#include <map>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELocationType,
    (Store)
    (Cache)
);

class TLocation
    : public TRefCounted
{
public:
    TLocation(
        ELocationType type,
        const Stroka& id,
        TLocationConfigBasePtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Returns the type.
    ELocationType GetType() const;

    //! Returns string id.
    const Stroka& GetId() const;

    //! Returns the profiler tagged with location id.
    const NProfiling::TProfiler& GetProfiler() const;

    //! Returns the root path of the location.
    Stroka GetPath() const;

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

    //! Returns |true| iff the location is enabled.
    bool IsEnabled() const;

    //! Marks the location as disabled.
    void Disable(const TError& reason);

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

    //! Changes the number of currently active sessions by a given delta.
    void UpdateSessionCount(int delta);

    //! Changes the number of chunks by a given delta.
    void UpdateChunkCount(int delta);

    //! Returns the number of currently active sessions.
    int GetSessionCount() const;

    //! Returns the number of chunks.
    int GetChunkCount() const;

    //! Returns a full path for a primary chunk file.
    Stroka GetChunkPath(const TChunkId& chunkId) const;

    //! Permanently removes the files comprising a given chunk.
    void RemoveChunkFilesPermanently(const TChunkId& chunkId);

    //! Removes a chunk permanently or moves it to the trash (if available).
    virtual void RemoveChunkFiles(const TChunkId& chunkId, bool force);

protected:
    mutable NLogging::TLogger Logger;
    NCellNode::TBootstrap* const Bootstrap_;


    static Stroka GetRelativeChunkPath(const TChunkId& chunkId);

    virtual bool ShouldSkipFileName(const Stroka& fileName) const;

    virtual void DoStart();

private:
    const ELocationType Type_;
    const Stroka Id_;
    const TLocationConfigBasePtr Config_;
    NProfiling::TProfiler Profiler_;

    std::atomic<bool> Enabled_ = {false};

    mutable i64 AvailableSpace_ = 0;
    i64 UsedSpace_ = 0;
    int SessionCount_ = 0;
    int ChunkCount_ = 0;

    const NConcurrency::TFairShareActionQueuePtr ReadQueue_;
    const IPrioritizedInvokerPtr DataReadInvoker_;
    const IPrioritizedInvokerPtr MetaReadInvoker_;

    const NConcurrency::TThreadPoolPtr WriteThreadPool_;
    const IInvokerPtr WritePoolInvoker_;

    const TDiskHealthCheckerPtr HealthChecker_;


    void CheckMinimumSpace();
    void CheckLockFile();

    void OnHealthCheckFailed(const TError& error);
    void MakeDisabled();

    i64 GetTotalSpace() const;

    virtual i64 GetAdditionalSpace() const;

    virtual TNullable<TChunkDescriptor> RepairChunk(const TChunkId& chunkId) = 0;

    virtual std::vector<Stroka> GetChunkPartNames(const TChunkId& chunkId) const = 0;
    virtual void DoAdditionalScan();

    std::vector<TChunkDescriptor> DoScan();
};

DEFINE_REFCOUNTED_TYPE(TLocation);

////////////////////////////////////////////////////////////////////////////////

class TStoreLocation
    : public TLocation
{
public:
    TStoreLocation(
        const Stroka& id,
        TStoreLocationConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Returns Journal Manager accociated with this location.
    TJournalManagerPtr GetJournalManager();

    //! Returns the space reserved for low watermark.
    //! Never throws.
    i64 GetLowWatermarkSpace() const;

    //! Checks whether the location is full.
    bool IsFull() const;

    //! Checks whether to location has enough space to contain file of size #size
    bool HasEnoughSpace(i64 size) const;

    //! Returns |true| if the location accepts new chunks of a given type.
    bool IsChunkTypeAccepted(NObjectClient::EObjectType chunkType);

    //! Removes a chunk permanently or moves it to the trash.
    virtual void RemoveChunkFiles(const TChunkId& chunkId, bool force) override;

private:
    const TStoreLocationConfigPtr Config_;

    const TJournalManagerPtr JournalManager_;

    struct TTrashChunkEntry
    {
        TChunkId ChunkId;
        i64 DiskSpace;
    };

    TSpinLock TrashMapSpinLock_;
    std::multimap<TInstant, TTrashChunkEntry> TrashMap_;
    i64 TrashDiskSpace_ = 0;
    const NConcurrency::TPeriodicExecutorPtr TrashCheckExecutor_;


    Stroka GetTrashPath() const;
    Stroka GetTrashChunkPath(const TChunkId& chunkId) const;
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

    virtual std::vector<Stroka> GetChunkPartNames(const TChunkId& chunkId) const override;
    virtual bool ShouldSkipFileName(const Stroka& fileName) const override;
    virtual void DoAdditionalScan() override;

    virtual void DoStart() override;
};

DEFINE_REFCOUNTED_TYPE(TStoreLocation);

////////////////////////////////////////////////////////////////////////////////

class TCacheLocation
    : public TLocation
{
public:
    TCacheLocation(
        const Stroka& id,
        TCacheLocationConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

private:
    const TCacheLocationConfigPtr Config_;

    TNullable<TChunkDescriptor> Repair(const TChunkId& chunkId, const Stroka& metaSuffix);
    virtual TNullable<TChunkDescriptor> RepairChunk(const TChunkId& chunkId) override;

    virtual std::vector<Stroka> GetChunkPartNames(const TChunkId& chunkId) const override;
};

DEFINE_REFCOUNTED_TYPE(TCacheLocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

