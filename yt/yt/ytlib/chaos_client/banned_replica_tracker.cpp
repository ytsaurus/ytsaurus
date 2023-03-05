#include "banned_replica_tracker.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosClient {

using namespace NTabletClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TBannedReplicaTracker
    : public IBannedReplicaTracker
{
public:
    explicit TBannedReplicaTracker(NLogging::TLogger logger)
        : Logger(std::move(logger))
    {
        YT_LOG_DEBUG("Banned replica tracker created");
    }

    bool IsReplicaBanned(TReplicaId replicaId) override
    {
        auto guard = Guard(SpinLock_);

        auto it = BannedReplicas_.find(replicaId);
        bool result = it != BannedReplicas_.end() && it->second.Counter > 0;

        YT_LOG_DEBUG("Banned replica tracker checking replica (ReplicaId: %v, Result: %v)",
            replicaId,
            result);

        return result;
    }

    TError GetReplicaError(TReplicaId replicaId) override
    {
        auto guard = Guard(SpinLock_);

        auto it = BannedReplicas_.find(replicaId);
        return it != BannedReplicas_.end() && it->second.Counter > 0
            ? it->second.Error
            : TError();
    }

    void BanReplica(TReplicaId replicaId, TError error) override
    {
        auto guard = Guard(SpinLock_);

        BannedReplicas_[replicaId] = TReplicaState{
            .Counter = static_cast<int>(std::ssize(BannedReplicas_)),
            .Error = std::move(error)
        };

        YT_LOG_DEBUG("Banned replica tracker has banned replica (ReplicaId: %v, ReplicasSize: %v)",
            replicaId,
            BannedReplicas_.size());
    }

    void SyncReplicas(const TReplicationCardPtr& replicationCard) override
    {
        auto guard = Guard(SpinLock_);

        auto replicaIds = GetKeys(BannedReplicas_);
        for (auto replicaId : replicaIds) {
            if (!replicationCard->Replicas.contains(replicaId)) {
                EraseOrCrash(BannedReplicas_, replicaId);
            }
        }

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
            if (!BannedReplicas_.contains(replicaId) &&
                replicaInfo.ContentType == ETableReplicaContentType::Data &&
                IsReplicaEnabled(replicaInfo.State))
            {
                InsertOrCrash(BannedReplicas_, std::make_pair(replicaId, TReplicaState{}));
            }
        }

        YT_LOG_DEBUG("Banned replica tracker synced replicas (Replicas: %v)",
            BannedReplicas_.size());

        DecreaseCounters();
    }

private:
    struct TReplicaState
    {
        int Counter = 0;
        TError Error;
    };

    const NLogging::TLogger Logger;

    THashMap<TReplicaId, TReplicaState> BannedReplicas_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void DecreaseCounters()
    {
        for (auto& [_, state] : BannedReplicas_) {
            if (state.Counter > 0) {
                --state.Counter;
            }
            if (state.Counter == 0) {
                state.Error = TError();
            }
        }
    }
};

IBannedReplicaTrackerPtr CreateBannedReplicaTracker(NLogging::TLogger logger)
{
    return New<TBannedReplicaTracker>(std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyBannedReplicaTracker
    : public IBannedReplicaTracker
{
public:
    TEmptyBannedReplicaTracker()
    { }

    bool IsReplicaBanned(TReplicaId /*replicaId*/) override
    {
        return false;
    }

    TError GetReplicaError(TReplicaId /*replicaId*/) override
    {
        return {};
    }

    void BanReplica(TReplicaId /*replicaId*/, TError /*error*/) override
    { }

    void SyncReplicas(const TReplicationCardPtr& /*replicationCard*/) override
    { }
};

IBannedReplicaTrackerPtr CreateEmptyBannedReplicaTracker()
{
    return New<TEmptyBannedReplicaTracker>();
}

////////////////////////////////////////////////////////////////////////////////

class TBannedReplicaTrackerCacheValue
    : public TSyncCacheValueBase<TTableId, IBannedReplicaTracker>
{
public:
    TBannedReplicaTrackerCacheValue(TTableId tableId, IBannedReplicaTrackerPtr tracker)
        : TSyncCacheValueBase(tableId)
        , Tracker_(std::move(tracker))
    { }

    const IBannedReplicaTrackerPtr& GetValue()
    {
        return Tracker_;
    }

private:
    IBannedReplicaTrackerPtr Tracker_;
};

class TBannedReplicaTrackerCache
    : public IBannedReplicaTrackerCache
    , public TSyncSlruCacheBase<TTableId, TBannedReplicaTrackerCacheValue>
{
public:
    TBannedReplicaTrackerCache(
        TSlruCacheConfigPtr config,
        NLogging::TLogger logger)
        : TSyncSlruCacheBase(std::move(config))
        , Logger(std::move(logger))
    { }

    IBannedReplicaTrackerPtr GetTracker(TTableId tableId) override
    {
        auto result = Find(tableId);
        if (result) {
            return result->GetValue();
        }

        auto tracker = CreateBannedReplicaTracker(Logger.WithTag("TableId: %v", tableId));
        TryInsert(New<TBannedReplicaTrackerCacheValue>(tableId, tracker));

        return tracker;
    }

    void Reconfigure(const TSlruCacheDynamicConfigPtr& config) override
    {
        TSyncSlruCacheBase::Reconfigure(config);
    }

private:
    const NLogging::TLogger Logger;
};

IBannedReplicaTrackerCachePtr CreateBannedReplicaTrackerCache(
    TSlruCacheConfigPtr config,
    NLogging::TLogger logger)
{
    return New<TBannedReplicaTrackerCache>(std::move(config), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
