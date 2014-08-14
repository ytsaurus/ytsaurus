#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>

#include <core/actions/signal.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <ytlib/chunk_client/chunk_info.pb.h>

#include <server/cell_node/public.h>

#include <atomic>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELocationType,
    (Store)
    (Cache)
);

//! Describes a physical location of chunks.
class TLocation
    : public TRefCounted
{
public:
    TLocation(
        ELocationType type,
        const Stroka& id,
        TLocationConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    ~TLocation();

    //! Returns the type.
    ELocationType GetType() const;

    //! Returns string id.
    const Stroka& GetId() const;

    //! Scan the location directory removing orphaned files and returning the list of found chunks.
    //! If the scan fails, the location becomes disabled, |Disabled| signal is raised, and an empty list is returned.
    std::vector<TChunkDescriptor> Initialize();

    //! Updates #UsedSpace and #AvailalbleSpace
    void UpdateUsedSpace(i64 size);

    //! Updates #AvailalbleSpace with a system call and returns the result.
    //! Never throws.
    i64 GetAvailableSpace() const;

    //! Returns the total space on the disk drive where the location resides.
    //! Never throws.
    i64 GetTotalSpace() const;

    //! Returns the number of bytes used at the location.
    /*!
     *  \note
     *  This may exceed #GetQuota.
     */
    i64 GetUsedSpace() const;

    //! Returns the maximum number of bytes the chunks assigned to this location
    //! are allowed to use.
    i64 GetQuota() const;

    //! Returns the path of the location.
    Stroka GetPath() const;

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

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const TChunkId& chunkId) const;

    //! Checks whether the location is full.
    bool IsFull() const;

    //! Checks whether to location has enough space to contain file of size #size
    bool HasEnoughSpace(i64 size) const;

    //! Returns an invoker for reading chunk data.
    IPrioritizedInvokerPtr GetDataReadInvoker();

    //! Returns an invoker for reading chunk meta.
    IPrioritizedInvokerPtr GetMetaReadInvoker();

    //! Returns an invoker for writing chunks.
    IInvokerPtr GetWriteInvoker();

    //! Returns |true| iff the location is enabled.
    bool IsEnabled() const;

    //! Marks the location as disabled.
    void Disable(TError reason);

    //! Raised when the location gets disabled.
    /*!
     *  Raised at most once in Control thread.
     */
    DEFINE_SIGNAL(void(const TError&), Disabled);

    //! The profiler tagged with location id.
    DEFINE_BYREF_RW_PROPERTY(NProfiling::TProfiler, Profiler);

private:
    ELocationType Type_;
    Stroka Id_;
    TLocationConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    std::atomic<bool> Enabled_;

    mutable i64 AvailableSpace_;
    i64 UsedSpace_;
    int SessionCount_;
    int ChunkCount_;

    NConcurrency::TFairShareActionQueuePtr ReadQueue_;
    IPrioritizedInvokerPtr DataReadInvoker_;
    IPrioritizedInvokerPtr MetaReadInvoker_;

    NConcurrency::TThreadPoolPtr WriteQueue_;
    IInvokerPtr WriteInvoker_;

    TDiskHealthCheckerPtr HealthChecker_;

    mutable NLog::TLogger Logger;


    std::vector<TChunkDescriptor> DoInitialize();
    TNullable<TChunkDescriptor> TryGetBlobDescriptor(const TChunkId& chunkId);
    TNullable<TChunkDescriptor> TryGetJournalDescriptor(const TChunkId& chunkId);

    void OnHealthCheckFailed(TError error);
    void ScheduleDisable(TError reason);
    void DoDisable(TError reason);

};

DEFINE_REFCOUNTED_TYPE(TLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

