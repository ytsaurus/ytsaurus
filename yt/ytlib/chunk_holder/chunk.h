#pragma once

#include "common.h"
#include "chunk.pb.h"

#include "../misc/property.h"
#include "../misc/error.h"
#include "../misc/cache.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////
   
class TLocation;

//! Chunk properties that can be obtained by the filesystem scan.
struct TChunkDescriptor
{
    TChunkId Id;
    i64 Size;
};

//! Describes chunk at a chunk holder.
class TChunk
    : public virtual TRefCountedBase
{
    //! Chunk id.`
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, Id);
    //! Chunk location.
    DEFINE_BYVAL_RO_PROPERTY(TIntrusivePtr<TLocation>, Location);
    //! The physical chunk size (including data and meta).
    DEFINE_BYVAL_RO_PROPERTY(i64, Size);

public:
    typedef TIntrusivePtr<TChunk> TPtr;

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
    TAsyncGetInfoResult::TPtr GetInfo() const;

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
    typedef TIntrusivePtr<TStoredChunk> TPtr;

    TStoredChunk(
        TLocation* location,
        const NProto::TChunkInfo& info);

    TStoredChunk(
        TLocation* location,
        const TChunkDescriptor& descriptor);
};

////////////////////////////////////////////////////////////////////////////////

//! A chunk owned by TChunkCache.
class TCachedChunk
    : public TChunk
    , public TCacheValueBase<TChunkId, TCachedChunk>
{
public:
    typedef TIntrusivePtr<TCachedChunk> TPtr;

    TCachedChunk(
        TLocation* location,
        const NProto::TChunkInfo& info);

    TCachedChunk(
        TLocation* location,
        const TChunkDescriptor& descriptor);

    ~TCachedChunk();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

