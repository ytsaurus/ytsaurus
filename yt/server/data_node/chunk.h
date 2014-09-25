#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/ref.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_info.pb.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

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

struct IChunk
    : public virtual TRefCounted
{
    virtual const TChunkId& GetId() const = 0;
    virtual TLocationPtr GetLocation() const = 0;
    virtual NChunkClient::NProto::TChunkInfo GetInfo() const = 0;

    virtual int GetVersion() const = 0;
    virtual void IncrementVersion() = 0;

    //! Returns |true| iff there is an active session for this chunk.
    virtual bool IsActive() const = 0;

    //! Returns the full path to the chunk data file.
    virtual Stroka GetFileName() const = 0;

    typedef TErrorOr<std::vector<TSharedRef>> TReadBlocksResult;
    typedef TFuture<TReadBlocksResult> TAsyncReadBlocksResult;

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
    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) = 0;

    //! Asynchronously reads a range of blocks.
    virtual TAsyncReadBlocksResult ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority) = 0;

    //! Tries to acquire a read lock and increments the lock counter.
    /*!
     *  Succeeds if removal is not scheduled yet.
     *  Returns |true| on success, |false| on failure.
     */
    virtual bool TryAcquireReadLock() = 0;

    //! Releases an earlier acquired read lock, decrements the lock counter.
    /*!
     *  If this was the last read lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
     */
    virtual void ReleaseReadLock() = 0;

    //! Returns |true| iff a read lock is acquired.
    virtual bool IsReadLockAcquired() const = 0;

    //! Marks the chunk as pending for removal.
    /*!
     *  After this call, no new read locks can be taken.
     *  If no read lock is currently in progress, enqueues removal actions
     *  to the appropriate thread.
     */
    virtual TFuture<void> ScheduleRemove() = 0;

    //! Performs synchronous physical removal of chunk files.
    //! For journal chunks this call bypasses multiplexed changelogs.
    //! Only called during initialization.
    virtual void SyncRemove() = 0;

    //! Returns the instance cast to TJournalChunk. Fails if cast is not possible.
    TJournalChunkPtr AsJournalChunk();

};

DEFINE_REFCOUNTED_TYPE(IChunk)

////////////////////////////////////////////////////////////////////////////////

class TChunkReadGuard
{
public:
    TChunkReadGuard() = default;
    TChunkReadGuard(TChunkReadGuard&& other) = default;
    ~TChunkReadGuard();

    TChunkReadGuard& operator = (TChunkReadGuard&& other);

    explicit operator bool() const;

    friend void swap(TChunkReadGuard& lhs, TChunkReadGuard& rhs);

    static TChunkReadGuard TryAcquire(IChunkPtr chunk);

private:
    explicit TChunkReadGuard(IChunkPtr chunk);

    IChunkPtr Chunk_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

