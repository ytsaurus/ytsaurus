#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

struct IShuffleController
    : public TRefCounted
{
    virtual TFuture<void> RegisterChunks(
        std::vector<NChunkClient::TInputChunkPtr> chunks,
        std::optional<int> writerIndex,
        bool overwriteExistingWriterData) = 0;

    virtual TFuture<std::vector<NChunkClient::TInputChunkSlicePtr>> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShuffleController)

////////////////////////////////////////////////////////////////////////////////

IShuffleControllerPtr CreateShuffleController(
    int partitionCount,
    IInvokerPtr invoker,
    NApi::ITransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
