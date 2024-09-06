#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardWatcherClientCallbacks
{
    virtual ~IReplicationCardWatcherClientCallbacks() = default;

    virtual void OnReplicationCardUpdated(
        TReplicationCardId replicationCardId,
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnReplicationCardDeleted(TReplicationCardId replicationCardId) = 0;

    virtual void OnUnknownReplicationCard(TReplicationCardId replicationCardId) = 0;

    virtual void OnNothingChanged(TReplicationCardId replicationCardId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardsWatcherClient
    : public virtual TRefCounted
{
    virtual void WatchReplicationCard(TReplicationCardId replicationCardId) = 0;
    virtual void StopWatchingReplicationCard(TReplicationCardId replicationCardId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardsWatcherClient)

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClient(
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
    TWeakPtr<NApi::NNative::IConnection> connection);

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClient(
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
    NRpc::IChannelPtr chaosCacheChannel,
    TWeakPtr<NApi::NNative::IConnection> connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
