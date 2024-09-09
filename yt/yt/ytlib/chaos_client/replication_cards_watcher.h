#pragma once

#include "public.h"

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

constexpr auto MinimalFetchOptions = TReplicationCardFetchOptions{
    .IncludeCoordinators = true,
    .IncludeHistory = true,
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EReplicationCardWatherState, ui8,
    ((Normal)(0))
    ((Deleted)(1))
    ((Migrated)(2))
    ((Unknown)(3))
);

struct IReplicationCardsWatcher
    : public virtual TRefCounted
{
    using TCtxReplicationCardWatchPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqWatchReplicationCard,
        NChaosClient::NProto::TRspWatchReplicationCard
    >>;

    virtual void RegisterReplicationCard(
        const TReplicationCardId& replicationCardId,
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnReplcationCardUpdated(
        const TReplicationCardId& replicationCardId,
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnReplicationCardRemoved(const TReplicationCardId& replicationCardId) = 0;
    virtual void OnReplicationCardMigrated(
        const std::vector<std::pair<TReplicationCardId, NObjectClient::TCellId>>& replicationCardIds) = 0;

    virtual EReplicationCardWatherState WatchReplicationCard(
        const TReplicationCardId& replicationCardId,
        NTransactionClient::TTimestamp cacheTimestamp,
        TCtxReplicationCardWatchPtr context,
        bool allowUnregistered = false) = 0;

    virtual bool TryUnregisterReplicationCard(const TReplicationCardId& replicationCardId) = 0;
    virtual TInstant GetLastSeenWatchers(const TReplicationCardId& replicationCardId) = 0;

    virtual void Start(const std::vector<std::pair<TReplicationCardId, TReplicationCardPtr>>& replicationCards) = 0;
    virtual void Stop() = 0;

};

DEFINE_REFCOUNTED_TYPE(IReplicationCardsWatcher)

IReplicationCardsWatcherPtr CreateReplicationCardsWatcher(
    const TReplicationCardsWatcherConfigPtr& config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
