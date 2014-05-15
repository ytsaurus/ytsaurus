#pragma once

#include "public.h"

#include <core/misc/common.h>
#include <core/misc/ref.h>
#include <core/misc/error.h>

#include <core/actions/future.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! A basic interface for readings chunks from a suitable source.
struct IReader
    : public virtual TRefCounted
{
    //! Describes a result of #AsyncReadBlocks.
    typedef TErrorOr<std::vector<TSharedRef>> TReadResult;
    typedef TFuture<TReadResult> TAsyncReadResult;
    typedef TPromise<TReadResult> TAsyncReadPromise;

    //! Describes a result of #AsyncGetChunkInfo.
    typedef TErrorOr<NChunkClient::NProto::TChunkMeta> TGetMetaResult;
    typedef TFuture<TGetMetaResult> TAsyncGetMetaResult;
    typedef TPromise<TGetMetaResult> TAsyncGetMetaPromise;

    //! Asynchronously reads a given set of blocks.
    virtual TAsyncReadResult ReadBlocks(const std::vector<int>& blockIndexes) = 0;

    //! Asynchronously obtains a meta, possibly filtered by #partitionTag and #extensionTags.
    virtual TAsyncGetMetaResult GetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* extensionTags = nullptr) = 0;

    virtual TChunkId GetChunkId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
