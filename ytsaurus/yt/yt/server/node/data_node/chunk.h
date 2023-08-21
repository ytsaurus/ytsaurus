#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReadOptions
    : public NChunkClient::TClientChunkReadOptions
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
 *  Thread affinity: any
 */
struct IChunk
    : public virtual TRefCounted
{
    virtual TChunkId GetId() const = 0;
    virtual const TChunkLocationPtr& GetLocation() const = 0;
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
     */
    virtual TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags = std::nullopt) = 0;

    //! Asynchronously reads a set of blocks.
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TChunkReadOptions& options) = 0;

    //! Asynchronously reads a range of blocks.
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TChunkReadOptions& options) = 0;

    //! Prepares the underlying reader to read chunk fragments.
    /*!
     *  Read lock must be held.
     *
     *  If null future is returned then the underlying reader is already open and prepared (this is the fast path).
     *  Otherwise the caller must wait for the returned future to become set to access the underlying reader.
     *  If file has not been opened yet, will consider #useDirectIO as a hint to use DirectIO.
     */
    virtual TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options,
        bool useDirectIO) = 0;

    //! Prepares a block fragment read request to be passed to IIOEngine.
    /*!
     *  #PrepareToReadChunkFragments must be invoked and its returned future
     *  must be set prior to this call.
     */
    virtual NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor,
        bool useDirectIO) = 0;

    //! Tries to acquire a read lock; throws on failure.
    /*!
     *  Succeeds if removal is not scheduled yet.
     *  Concurrent update locks do not interfere with read locks.
     */
    virtual void AcquireReadLock() = 0;

    //! Releases an earlier acquired read lock, decrements the lock counter.
    /*!
     *  If this was the last read lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
     */
    virtual void ReleaseReadLock() = 0;

    //! Tries to acquire an update lock; throws on failure.
    /*!
     *  Succeeds if removal is not scheduled yet and no other update lock is
     *  currently taken.
     */
    virtual void AcquireUpdateLock() = 0;

    //! Releases an earlier acquired update lock.
    /*!
     *  If this was the last lock and chunk removal is pending,
     *  enqueues removal actions to the appropriate thread.
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
    virtual bool IsRemoveScheduled() const = 0;

    //! Performs synchronous physical removal of chunk files.
    //!
    /*  For journal chunks this call bypasses multiplexed changelogs.
     *  If #force is |true| then the files are just removed; otherwise these are move to the trash.
     */
    virtual void SyncRemove(bool force) = 0;

    //! See IChunkRegistry::ScheduleChunkReaderSweep.
    virtual void TrySweepReader() = 0;

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
    static TChunkReadGuard TryAcquire(IChunkPtr chunk);

    const IChunkPtr& GetChunk() const;

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

