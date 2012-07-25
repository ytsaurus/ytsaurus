#pragma once

#include "public.h"
#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/cache.h>

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
    //! The physical chunk size (including data and meta).
    DEFINE_BYVAL_RO_PROPERTY(NProto::TChunkInfo, Info);

public:
    //! Constructs a chunk for which its meta is already known.
    TChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NProto::TChunkMeta& chunkMeta,
        const NProto::TChunkInfo& chunkInfo);

    //! Constructs a chunk for which no info is loaded.
    TChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor);

    ~TChunk();

    //! Returns the full path to the chunk data file.
    Stroka GetFileName() const;

    typedef TValueOrError<NProto::TChunkMeta> TGetMetaResult;
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
    void ScheduleRemoval();


private:
    void Initialize();

    TFuture<TError> ReadMeta();

    mutable TSpinLock SpinLock;
    
    mutable volatile bool HasMeta;
    mutable NProto::TChunkMeta Meta;
    
    int ReadLockCounter;
    bool RemovalPending;
    bool RemovalScheduled;

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
        const NProto::TChunkMeta& chunkMeta,
        const NProto::TChunkInfo& chunkInfo);

    TStoredChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor);

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
        const NProto::TChunkMeta& chunkMeta,
        const NProto::TChunkInfo& chunkInfo,
        TChunkCachePtr chunkCache);

    TCachedChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        TChunkCachePtr chunkCache);

    ~TCachedChunk();

private:
    TWeakPtr<TChunkCache> ChunkCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

