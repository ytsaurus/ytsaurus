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

private:
    TFuture<TError> ReadMeta();

    mutable TSpinLock SpinLock;
    mutable volatile bool HasMeta;
    mutable NProto::TChunkMeta Meta;

};

////////////////////////////////////////////////////////////////////////////////

//! A chunk owned by #TChunkStore.
class TStoredChunk
    : public TChunk
{
public:
    TStoredChunk(
        TLocation* location,
        const TChunkId& chunkId,
        const NProto::TChunkMeta& chunkMeta,
        const NProto::TChunkInfo& chunkInfo);

    TStoredChunk(
        TLocation* location,
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
        TLocation* location,
        const TChunkId& chunkId,
        const NProto::TChunkMeta& chunkMeta,
        const NProto::TChunkInfo& chunkInfo,
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

