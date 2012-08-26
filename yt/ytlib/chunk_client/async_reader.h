#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/ref.h>
#include <ytlib/misc/error.h>

#include <ytlib/actions/future.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for readings chunks from holders.
struct IAsyncReader
    : public virtual TRefCounted
{
    //! Describes a result of #AsyncReadBlocks.
    typedef TValueOrError< std::vector<TSharedRef> > TReadResult;
    typedef TFuture<TReadResult> TAsyncReadResult;
    typedef TPromise<TReadResult> TAsyncReadPromise;

    //! Describes a result of #AsyncGetChunkInfo.
    typedef TValueOrError<NChunkClient::NProto::TChunkMeta> TGetMetaResult;
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
        const std::vector<int>* tags = NULL) = 0;

    virtual TChunkId GetChunkId() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
