#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

struct IShuffleManager
    : public TRefCounted
{
    virtual TFuture<NObjectClient::TTransactionId> StartShuffle(
        int partitionCount,
        NObjectClient::TTransactionId parentTransactionId) = 0;

    virtual TFuture<void> FinishShuffle(NObjectClient::TTransactionId transactionId) = 0;

    virtual TFuture<void> RegisterChunks(
        NObjectClient::TTransactionId transactionId,
        std::vector<NChunkClient::TInputChunkPtr> chunks,
        std::optional<int> writerIndex,
        bool overwriteExistingWriterData) = 0;

    virtual TFuture<std::vector<NChunkClient::TInputChunkSlicePtr>> FetchChunks(
        NObjectClient::TTransactionId transactionId,
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShuffleManager)

////////////////////////////////////////////////////////////////////////////////

IShuffleManagerPtr CreateShuffleManager(
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
