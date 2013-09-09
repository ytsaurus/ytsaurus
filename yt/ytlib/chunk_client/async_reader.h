#pragma once

#include <core/misc/common.h>
#include <core/misc/ref.h>
#include <core/misc/error.h>

#include <core/actions/future.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for readings chunks from data nodes.
struct IAsyncReader
    : public virtual TRefCounted
{
    //! Describes a result of #AsyncReadBlocks.
    typedef TErrorOr< std::vector<TSharedRef> > TReadResult;
    typedef TFuture<TReadResult> TAsyncReadResult;
    typedef TPromise<TReadResult> TAsyncReadPromise;

    //! Describes a result of #AsyncGetChunkInfo.
    typedef TErrorOr<NChunkClient::NProto::TChunkMeta> TGetMetaResult;
    typedef TFuture<TGetMetaResult> TAsyncGetMetaResult;
    typedef TPromise<TGetMetaResult> TAsyncGetMetaPromise;

    //! Reads (asynchronously) a given set of blocks.
    /*!
     *  Negative indexes indicate that blocks are numbered from the end.
     *  I.e. -1 means the last block.
     */
    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) = 0;

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = nullptr) = 0;

    virtual TChunkId GetChunkId() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
