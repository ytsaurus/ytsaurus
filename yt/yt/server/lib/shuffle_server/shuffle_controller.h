#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

struct IShuffleController
    : public TRefCounted
{
    virtual TFuture<void> RegisterChunks(std::vector<NChunkClient::TInputChunkPtr> chunks) = 0;

    virtual TFuture<std::vector<NChunkClient::TInputChunkSlicePtr>> FetchChunks(int partitionIndex) = 0;

    virtual NObjectClient::TTransactionId GetTransactionId() const = 0;

    virtual TFuture<void> Finish() = 0;
};

DEFINE_REFCOUNTED_TYPE(IShuffleController)

////////////////////////////////////////////////////////////////////////////////

TFuture<IShuffleControllerPtr> CreateShuffleController(
    NApi::NNative::IClientPtr client,
    int partitionCount,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
