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
    //! Chunk id.`
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, Id);
    //! Chunk location.
    DEFINE_BYVAL_RO_PROPERTY(TIntrusivePtr<TLocation>, Location);
    //! The physical chunk size (including data and meta).
    DEFINE_BYVAL_RO_PROPERTY(i64, Size);

public:
    //! Constructs a chunk for which its info is already known.
    TChunk(
        TLocation* location,
        const NChunkHolder::NProto::TChunkInfo& info);

    //! Constructs a chunk for which no info is loaded.
    TChunk(
        TLocation* location,
        const TChunkDescriptor& descriptor);

    //! Returns the full path to the chunk data file.
    Stroka GetFileName() const;

    typedef TValueOrError<NChunkHolder::NProto::TChunkInfo> TGetInfoResult;
    typedef TFuture<TGetInfoResult> TAsyncGetInfoResult;

    //! Returns chunk info.
    /*!
     *  The info is fetched asynchronously and is cached.
     */
    TAsyncGetInfoResult GetInfo();

private:
    mutable TSpinLock SpinLock;
    mutable volatile bool HasInfo;
    mutable NChunkHolder::NProto::TChunkInfo Info;

};

////////////////////////////////////////////////////////////////////////////////

//! A chunk owned by #TChunkStore.
class TStoredChunk
    : public TChunk
{
public:
    TStoredChunk(
        TLocation* location,
        const NProto::TChunkInfo& info);

    TStoredChunk(
        TLocation* location,
        const TChunkDescriptor& descriptor);
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
        TLocation* location,
        const NProto::TChunkInfo& info,
        TChunkCache* chunkCache);

    TCachedChunk(
        TLocation* location,
        const TChunkDescriptor& descriptor,
        TChunkCache* chunkCache);

    ~TCachedChunk();

private:
    TWeakPtr<TChunkCache> ChunkCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

