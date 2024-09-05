#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EReplicationCardWatherState, ui8,
    ((Normal)   (0))
    ((Deleted)  (1))
    ((Migrated) (2))
    ((Unknown)  (3))
);

struct IReplicationCardWatcherCallbacks
    : public virtual TRefCounted
{
    virtual void OnReplicationCardChanged(
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnReplicationCardMigrated(NElection::TCellId destination) = 0;
    virtual void OnReplicationCardDeleted() = 0;
    virtual void OnInstanceIsNotLeader() = 0;
    virtual void OnNothingChanged() = 0;
    virtual void OnUnknownReplicationCard() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardWatcherCallbacks)

struct IReplicationCardsWatcher
    : public virtual TRefCounted
{
    virtual void RegisterReplicationCard(
        TReplicationCardId replicationCardId,
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnReplcationCardUpdated(
        TReplicationCardId replicationCardId,
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnReplicationCardRemoved(TReplicationCardId replicationCardId) = 0;
    virtual void OnReplicationCardMigrated(
        const std::vector<std::pair<TReplicationCardId, NObjectClient::TCellId>>& replicationCardIds) = 0;

    virtual EReplicationCardWatherState WatchReplicationCard(
        TReplicationCardId replicationCardId,
        NTransactionClient::TTimestamp cacheTimestamp,
        IReplicationCardWatcherCallbacksPtr callbacks,
        bool allowUnregistered = false) = 0;

    virtual bool TryUnregisterReplicationCard(TReplicationCardId replicationCardId) = 0;
    virtual TInstant GetLastSeenWatchers(TReplicationCardId replicationCardId) = 0;

    virtual void Start(const std::vector<std::pair<TReplicationCardId, TReplicationCardPtr>>& replicationCards) = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardsWatcher)

IReplicationCardsWatcherPtr CreateReplicationCardsWatcher(
    TReplicationCardsWatcherConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
