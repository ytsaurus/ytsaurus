#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/block.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TBlockReadOptions
    : public NChunkClient::TClientBlockReadOptions
{
    NChunkClient::IBlockCachePtr BlockCache;
    bool PopulateCache = false;
    bool FetchFromCache = true;
    bool FetchFromDisk = true;
};

//! Represents a chunk stored locally at Data Node.
/*!
 *  \note
 *  Thread affinity: ControlThread (unless indicated otherwise)
 */
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

    //! Marks the chunk as dead, e.g. removed.
    virtual void SetDead() = 0;

    //! Returns |true| if the chunk is still registered.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsAlive() const = 0;

    //! Returns the full path to the chunk data file.
    virtual TString GetFileName() const = 0;

    //! Returns chunk meta.
    /*!
     *  \note
     *  The meta is fetched asynchronously and is cached.
     *  Thread affinity: any
     */
    virtual TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TBlockReadOptions& options,
        const TNullable<std::vector<int>>& extensionTags = Null) = 0;

    //! Asynchronously reads a set of blocks.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TBlockReadOptions& options) = 0;

    //! Asynchronously reads a range of blocks.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TBlockReadOptions& options) = 0;

    //! Tries to acquire a read lock and increments the lock counter.
    /*!
     *  Succeeds if removal is not scheduled yet.
     *  Returns |true| on success, |false| on failure.
     *
     *  \note
     *  Thread affinity: any
     */
    virtual bool TryAcquireReadLock() = 0;

    //! Releases an earlier acquired read lock, decrements the lock counter.
    /*!
     *  If this was the last read lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
     *
     *  \note
     *  Thread affinity: any
     */
    virtual void ReleaseReadLock() = 0;

    //! Returns |true| iff a read lock is acquired.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsReadLockAcquired() const = 0;

    //! Marks the chunk as pending for removal.
    /*!
     *  After this call, no new read locks can be taken.
     *  If no read lock is currently in progress, enqueues removal actions
     *  to the appropriate thread.
     */
    virtual TFuture<void> ScheduleRemove() = 0;

    //! Returns |true| if #ScheduleRemove was called.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsRemoveScheduled() const = 0;

    //! Performs synchronous physical removal of chunk files.
    //!
    /*  For journal chunks this call bypasses multiplexed changelogs.
     *  If #force is |true| then the files are just removed; otherwise these are move to the trash.
     */
    virtual void SyncRemove(bool force) = 0;

    //! Returns the object type extracted from chunk id.
    NObjectClient::EObjectType GetType() const;

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
    static TChunkReadGuard AcquireOrThrow(IChunkPtr chunk);

private:
    explicit TChunkReadGuard(IChunkPtr chunk);

    IChunkPtr Chunk_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

