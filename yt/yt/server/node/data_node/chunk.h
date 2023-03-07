#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/block.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TBlockReadOptions
    : public NChunkClient::TClientBlockReadOptions
{
    NChunkClient::IBlockCachePtr BlockCache;
    bool PopulateCache = false;
    bool FetchFromCache = true;
    bool FetchFromDisk = true;
    //! By this moment read routine is advised to return at least anything in best-effort manner.
    //! Failure to do so may result in RPC timeout or other kind of lower-level error.
    TInstant Deadline = TInstant::Max();
};

//! Represents a chunk stored locally at Data Node.
/*!
 *  \note
 *  Thread affinity: ControlThread (unless indicated otherwise)
 */
struct IChunk
    : public virtual TRefCounted
{
    virtual TChunkId GetId() const = 0;
    virtual const TLocationPtr& GetLocation() const = 0;
    virtual NChunkClient::NProto::TChunkInfo GetInfo() const = 0;

    virtual int GetVersion() const = 0;
    virtual int IncrementVersion() = 0;

    //! Returns |true| iff there is an active session for this chunk.
    /*!
     *  For blob chunks this is always |false|.
     */
    virtual bool IsActive() const = 0;

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
        const std::optional<std::vector<int>>& extensionTags = std::nullopt) = 0;

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

    //! Tries to acquire a read lock; throws on failure.
    /*!
     *  Succeeds if removal is not scheduled yet.
     *  Concurrent update locks do not interfere with read locks.
     *  Returns |true| on success, |false| on failure.
     *
     *  \note
     *  Thread affinity: any
     */
    virtual void AcquireReadLock() = 0;

    //! Releases an earlier acquired read lock, decrements the lock counter.
    /*!
     *  If this was the last read lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
     *
     *  \note
     *  Thread affinity: any
     */
    virtual void ReleaseReadLock() = 0;

    //! Tries to acquire an update lock; throws on failure.
    /*!
     *  Succeeds if removal is not scheduled yet and no other update lock is
     *  currently taken.
     *
     *  \note
     *  Thread affinity: any
     */
    virtual void AcquireUpdateLock() = 0;

    //! Releases an earlier acquired update lock.
    /*!
     *  If this was the last lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
     *
     *  \note
     *  Thread affinity: any
     */
    virtual void ReleaseUpdateLock() = 0;
 
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

    //! Returns |true| is this is a journal type.
    bool IsJournalChunk() const;

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

    TChunkReadGuard& operator = (TChunkReadGuard&& other) = default;

    explicit operator bool() const;

    static TChunkReadGuard Acquire(IChunkPtr chunk);

private:
    explicit TChunkReadGuard(IChunkPtr chunk);

    IChunkPtr Chunk_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkUpdateGuard
{
public:
    TChunkUpdateGuard() = default;
    TChunkUpdateGuard(TChunkUpdateGuard&& other) = default;
    ~TChunkUpdateGuard();

    TChunkUpdateGuard& operator = (TChunkUpdateGuard&& other) = default;

    explicit operator bool() const;

    static TChunkUpdateGuard Acquire(IChunkPtr chunk);

private:
    explicit TChunkUpdateGuard(IChunkPtr chunk);

    IChunkPtr Chunk_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

