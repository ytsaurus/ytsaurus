#pragma once

#include "public.h"
#include "shuffle_controller.h"

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
        NObjectClient::TTransactionId parentTransactionId,
        bool usePushBasedShuffle,
        std::string account,
        std::string medium,
        int replicationFactor,
        NPushBasedShuffleClient::TPushShuffleConfigPtr pushConfig) = 0;

    virtual TFuture<void> FinishShuffle(NObjectClient::TTransactionId transactionId) = 0;

    //! Returns the controller for a shuffle; callers downcast it to
    //! IPullBasedShuffleController or IPushBasedShuffleController as appropriate.
    virtual TFuture<IShuffleControllerPtr> GetController(NObjectClient::TTransactionId transactionId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IShuffleManager)

////////////////////////////////////////////////////////////////////////////////

IShuffleManagerPtr CreateShuffleManager(
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
