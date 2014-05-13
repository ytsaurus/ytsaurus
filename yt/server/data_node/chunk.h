#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/error.h>
#include <core/misc/cache.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_info.pb.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained by the filesystem scan.
struct TChunkDescriptor
{
    TChunkId Id;
    i64 DiskSpace;
};

class TRefCountedChunkMeta
    : public TIntrinsicRefCounted
    , public NChunkClient::NProto::TChunkMeta
{
public:
    TRefCountedChunkMeta();
    TRefCountedChunkMeta(const TRefCountedChunkMeta& other);
    TRefCountedChunkMeta(TRefCountedChunkMeta&& other);

    explicit TRefCountedChunkMeta(const NChunkClient::NProto::TChunkMeta& other);
    explicit TRefCountedChunkMeta(NChunkClient::NProto::TChunkMeta&& other);

};

DEFINE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public virtual TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TLocationPtr, Location);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::NProto::TChunkInfo, Info);

public:
    ~TChunk();

    //! Returns the full path to the chunk data file.
    Stroka GetFileName() const;

    typedef TErrorOr<TRefCountedChunkMetaPtr> TGetMetaResult;
    typedef TFuture<TGetMetaResult> TAsyncGetMetaResult;

    //! Returns chunk meta.
    /*!
     *  \param priority Request priority.
     *  \param tags The list of extension tags to return. If |nullptr|
     *  then all extensions are returned.
     *
     *  \note The meta is fetched asynchronously and is cached.
     */
    TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr);

    //! Returns chunk meta.
    /*!
        If chunk meta not cached, returns |nullptr|.
     */
    TRefCountedChunkMetaPtr GetCachedMeta() const;

    //! Tries to acquire a read lock and increments the lock counter.
    /*!
     *  Succeeds if removal is not scheduled yet.
     *  Returns |true| on success, |false| on failure.
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

    TChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

private:
    void Initialize();
    void DoRemoveChunk();

    TAsyncError ReadMeta(i64 priority);
    void DoReadMeta(TPromise<TError> promise);

    mutable TSpinLock SpinLock_;
    mutable TRefCountedChunkMetaPtr Meta_;

    int ReadLockCounter_;
    bool RemovalScheduled_;
    TPromise<void> RemovedEvent_;

    NCellNode::TNodeMemoryTracker* MemoryUsageTracker_;

};

DEFINE_REFCOUNTED_TYPE(TChunk)

////////////////////////////////////////////////////////////////////////////////

//! A chunk owned by TChunkStore.
class TStoredChunk
    : public TChunk
{
public:
    TStoredChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TStoredChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

};

DEFINE_REFCOUNTED_TYPE(TStoredChunk)

////////////////////////////////////////////////////////////////////////////////

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
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TCachedChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        TChunkCachePtr chunkCache,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    ~TCachedChunk();

private:
    TWeakPtr<TChunkCache> ChunkCache_;

};

DEFINE_REFCOUNTED_TYPE(TCachedChunk)

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunk
{
public:
    TJournalChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TJournalChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

