#pragma once

#include "client.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/library/erasure/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TReadHunkRequest
{
    NChunkClient::TChunkId ChunkId;
    NErasure::ECodec ErasureCodec = NErasure::ECodec::None;
    int BlockIndex = -1;
    i64 BlockOffset = -1;
    i64 Length = -1;
    std::optional<i64> BlockSize;
};

struct TReadHunksOptions
    : public TTimeoutOptions
{
    NChunkClient::TChunkFragmentReaderConfigPtr Config;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a set of private APIs.
/*!
 *  Only native clients are expected to implement this.
 */
struct IInternalClient
    : public virtual TRefCounted
{
    virtual TFuture<std::vector<TSharedRef>> ReadHunks(
        const std::vector<TReadHunkRequest>& requests,
        const TReadHunksOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInternalClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
