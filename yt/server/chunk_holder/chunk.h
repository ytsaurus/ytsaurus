#pragma once

#include "public.h"
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/cache.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained by the filesystem scan.
struct TChunkDescriptor
{
    TChunkId Id;
    i64 Size;
};

//! Describes chunk at a chunk holder.
class TChunk
    : public virtual TRefCounted
{
    //! Chunk id.
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, Id);
    //! Chunk location.
    DEFINE_BYVAL_RO_PROPERTY(TLocationPtr, Location);

    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::NProto::TChunkInfo, Info);

public:
    //! Constructs a chunk for which its meta is already known.
    TChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        NCellNode::TNodeMemoryTracker& memoryUsageTracker);

    //! Constructs a chunk for which no info is loaded.
    TChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker& memoryUsageTracker);

    ~TChunk();

    //! Returns the full path to the chunk data file.
    Stroka GetFileName() const;

    typedef TValueOrError<NChunkClient::NProto::TChunkMeta> TGetMetaResult;
    typedef TFuture<TGetMetaResult> TAsyncGetMetaResult;

    //! Returns chunk meta.
    /*!
     *  \param tags The list of extension tags to return. If NULL
     *  then all extensions are returned.
     *  
     *  \note The meta is fetched asynchronously and is cached.
     */
    TAsyncGetMetaResult GetMeta(const std::vector<int>* tags = NULL);

    //! Tries to acquire a read lock and increments the lock counter.
    /*!
     *  Succeeds if removal is not scheduled yet.
     *  Returns True on success, False on failure.
     */
    bool TryAcquireReadLock();

    //! Releases an earlier acquired read lock, decrements the lock counter.
    /*!
     *  If this was the last read lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
     */
    void ReleaseReadLock();

    //! Marks the chunk as pending for removal.
    /*!
     *  After this call, no new read locks can be taken.
     *  If no read lock is currently in progress, enqueues removal actions
     *  to the appropriate thread.
     */
    TFuture<void> ScheduleRemoval();

protected:
    void EvictChunkReader();


private:
    void Initialize();
    void DoRemoveChunk();

    TAsyncError ReadMeta();

    mutable TSpinLock SpinLock;
    
    mutable volatile bool HasMeta;
    mutable NChunkClient::NProto::TChunkMeta Meta;
    
    int ReadLockCounter;
    bool RemovalScheduled;
    TPromise<void> RemovedEvent;

    NCellNode::TNodeMemoryTracker& MemoryUsageTracker;

};

////////////////////////////////////////////////////////////////////////////////

//! A chunk owned by #TChunkStore.
class TStoredChunk
    : public TChunk
{
public:
    TStoredChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        NCellNode::TNodeMemoryTracker& memoryUsageTracker);

    TStoredChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker& memoryUsageTracker);

    ~TStoredChunk();
};

////////////////////////////////////////////////////////////////////////////////

class TChunkCache;

//! A chunk owned by TChunkCache.
class TCachedChunk
    : public TChunk
    , public TCacheValueBase<TChunkId, TCachedChunk>
{
public:
    TCachedChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        TChunkCachePtr chunkCache,
        NCellNode::TNodeMemoryTracker& memoryUsageTracker);

    TCachedChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        TChunkCachePtr chunkCache,
        NCellNode::TNodeMemoryTracker& memoryUsageTracker);

    ~TCachedChunk();

private:
    TWeakPtr<TChunkCache> ChunkCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

