#include "table_puller_helpers.h"
#include "tablet.h"

#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NTabletClient;
using namespace NObjectClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TBannedReplicaTracker::TBannedReplicaTracker(TLogger logger, std::optional<int> replicaBanDuration)
    : Logger(std::move(logger))
    , ReplicaBanDuration_(replicaBanDuration)
{ }

bool TBannedReplicaTracker::IsReplicaBanned(TReplicaId replicaId) const
{
    auto it = BannedReplicas_.find(replicaId);
    bool result = it != BannedReplicas_.end() && it->second.Counter > 0;

    YT_LOG_TRACE("Banned replica tracker checking replica (ReplicaId: %v, Result: %v)",
        replicaId,
        result);

    return result;
}

void TBannedReplicaTracker::BanReplica(TReplicaId replicaId, TError error)
{
    BannedReplicas_[replicaId] = TBanInfo{ReplicaBanDuration_.value_or(std::size(BannedReplicas_)), std::move(error)};

    YT_LOG_DEBUG("Banned replica tracker has banned replica (ReplicaId: %v, ReplicasSize: %v)",
        replicaId,
        BannedReplicas_.size());
}

void TBannedReplicaTracker::SyncReplicas(const TReplicationCardPtr& replicationCard)
{
    auto replicaIds = GetKeys(BannedReplicas_);
    for (auto replicaId : replicaIds) {
        if (!replicationCard->Replicas.contains(replicaId)) {
            EraseOrCrash(BannedReplicas_, replicaId);
        }
    }

    for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
        if (!BannedReplicas_.contains(replicaId) &&
            replicaInfo.ContentType == ETableReplicaContentType::Queue &&
            IsReplicaEnabled(replicaInfo.State))
        {
            InsertOrCrash(BannedReplicas_, std::pair(replicaId, TBanInfo{0, TError()}));
        }
    }

    DecreaseCounters();
}

const THashMap<TReplicaId, TBannedReplicaTracker::TBanInfo>& TBannedReplicaTracker::GetBannedReplicas() const
{
    return BannedReplicas_;
}

void TBannedReplicaTracker::DecreaseCounters()
{
    for (auto& [_, info] : BannedReplicas_) {
        if (info.Counter > 0) {
            --info.Counter;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
const T& ChooseReplica(const std::vector<T>& candidates, const TReplicaInfo& selfReplica)
{
    const auto& selfClusterName = selfReplica.ClusterName;
    for (const auto& candidate : candidates) {
        if (std::get<1>(candidate)->ClusterName == selfClusterName) {
            return candidate;
        }
    }

    return candidates[RandomNumber(candidates.size())];
}

TQueueReplicaSelector::TQueueReplicaSelector(
    NLogging::TLogger logger,
    const TBannedReplicaTracker& bannedReplicaTracker)
    : Logger(std::move(logger))
    , BannedReplicaTracker_(bannedReplicaTracker)
    , LastPulledFromReplicaId_(NullObjectId)
    , NextPermittedTimeForProgressBehindAlert_(Now())
{ }

TQueueReplicaSelector::TReplicaOrError TQueueReplicaSelector::PickQueueReplica(
    TReplicaId selfUpstreamReplicaId,
    const TReplicationCardPtr& replicationCard,
    const TReplicationProgress& replicationProgress,
    TInstant now)
{
    // If our progress is less than any queue replica progress, pull from that replica.
    // Otherwise pull from sync replica of oldest era corresponding to our progress.

    YT_LOG_DEBUG("Pick replica to pull from");

    auto* selfReplica = replicationCard->FindReplica(selfUpstreamReplicaId);
    if (!selfReplica) {
        return TError("Will not pull rows since replication card does not contain us");
    }

    if (!IsReplicationProgressGreaterOrEqual(replicationProgress, selfReplica->ReplicationProgress)) {
        constexpr auto message = "Will not pull rows since actual replication progress is behind replication card replica progress";

        // TODO(ponasenko-rs): Remove alerts after testing period.
        if (now >= NextPermittedTimeForProgressBehindAlert_) {
            YT_LOG_ALERT("%s (ReplicationProgress: %v, ReplicaInfo: %v)",
                message,
                replicationProgress,
                *selfReplica);
            NextPermittedTimeForProgressBehindAlert_ = now + TDuration::Days(1);
        }

        return TError(message)
            << TErrorAttribute("replication_progress", replicationProgress)
            << TErrorAttribute("replica_info", *selfReplica);
    }

    auto oldestTimestamp = GetReplicationProgressMinTimestamp(replicationProgress);
    auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);
    if (historyItemIndex == -1) {
        return TError("Will not pull rows since replica history does not cover replication progress")
            << TErrorAttribute("oldest_timestamp", oldestTimestamp)
            << TErrorAttribute("history", selfReplica->History);
    }

    YT_VERIFY(historyItemIndex >= 0 && historyItemIndex < std::ssize(selfReplica->History));
    const auto& historyItem = selfReplica->History[historyItemIndex];
    if (historyItem.IsSync()) {
        return TError("Will not pull rows since oldest progress timestamp corresponds to sync history item")
            << TErrorAttribute("oldest_timestamp", oldestTimestamp)
            << TErrorAttribute("history_item", historyItem);
    }

    if (!IsReplicaAsync(selfReplica->Mode)) {
        YT_LOG_DEBUG("Pulling rows while replica is not async (ReplicaMode: %v)",
            selfReplica->Mode);
        // NB: Allow this since sync replica could be catching up.
    }

    auto findFreshQueueReplica = [&] () -> std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*> {
        std::vector<std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*>> candidates;
        for (auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
            if (BannedReplicaTracker_.IsReplicaBanned(replicaId)) {
                continue;
            }

            if (replicaInfo.ContentType != ETableReplicaContentType::Queue ||
                !IsReplicaEnabled(replicaInfo.State) ||
                replicaInfo.FindHistoryItemIndex(oldestTimestamp) == -1)
            {
                continue;
            }

            if (selfReplica->ContentType == ETableReplicaContentType::Data) {
                if (!IsReplicationProgressGreaterOrEqual(replicationProgress, replicaInfo.ReplicationProgress)) {
                    if (replicaId == LastPulledFromReplicaId_) {
                        return {replicaId, &replicaInfo};
                    }
                    candidates.emplace_back(replicaId, &replicaInfo);
                }
            } else {
                YT_VERIFY(selfReplica->ContentType == ETableReplicaContentType::Queue);
                auto replicaOldestTimestamp = GetReplicationProgressMinTimestamp(
                    replicaInfo.ReplicationProgress,
                    replicationProgress.Segments[0].LowerKey,
                    replicationProgress.UpperKey);
                if (replicaOldestTimestamp > oldestTimestamp) {
                    if (replicaId == LastPulledFromReplicaId_) {
                        return {replicaId, &replicaInfo};
                    }

                    candidates.emplace_back(replicaId, &replicaInfo);
                }
            }
        }

        if (!candidates.empty()) {
            return ChooseReplica(candidates, *selfReplica);
        }

        return {};
    };

    auto findSyncQueueReplica = [&] () -> std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*, TTimestamp> {
        std::vector<std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*, TTimestamp>> candidates;
        for (auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
            if (BannedReplicaTracker_.IsReplicaBanned(replicaId)) {
                continue;
            }

            if (replicaInfo.ContentType != ETableReplicaContentType::Queue || !IsReplicaEnabled(replicaInfo.State)) {
                continue;
            }

            auto historyItemIndex = replicaInfo.FindHistoryItemIndex(oldestTimestamp);
            if (historyItemIndex == -1) {
                continue;
            }

            const auto& historyItem = replicaInfo.History[historyItemIndex];
            if (!historyItem.IsSync()) {
                continue;
            }

            YT_LOG_DEBUG("Found sync replica corresponding history item (ReplicaId %v, HistoryItem: %v)",
                replicaId,
                historyItem);

            // Pull from (past) sync replica until it changed mode or we became sync.
            // AsyncToSync -> SyncToAsync transition is possible, so check the previous state
            // when in SyncToAsync mode
            auto upperTimestamp = NullTimestamp;
            if (historyItemIndex + 1 < std::ssize(replicaInfo.History)) {
                upperTimestamp = replicaInfo.History[historyItemIndex + 1].Timestamp;
            } else if (IsReplicaReallySync(selfReplica->Mode, selfReplica->State, selfReplica->History)) {
                upperTimestamp = selfReplica->History.back().Timestamp;
            }

            if (replicaId == LastPulledFromReplicaId_) {
                return {replicaId, &replicaInfo, upperTimestamp};
            }

            candidates.emplace_back(replicaId, &replicaInfo, upperTimestamp);
        }

        if (!candidates.empty()) {
            return ChooseReplica(candidates, *selfReplica);
        }

        return {};
    };

    if (auto [queueReplicaId, queueReplica] = findFreshQueueReplica(); queueReplica) {
        YT_LOG_DEBUG("Pull rows from fresh replica (ReplicaId: %v)",
            queueReplicaId);

        LastPulledFromReplicaId_ = queueReplicaId;
        return std::tuple{queueReplicaId, queueReplica, NullTimestamp};
    }

    if (auto [queueReplicaId, queueReplicaInfo, upperTimestamp] = findSyncQueueReplica(); queueReplicaInfo) {
        YT_LOG_DEBUG("Pull rows from sync replica (ReplicaId: %v, OldestTimestamp: %v, UpperTimestamp: %v)",
            queueReplicaId,
            oldestTimestamp,
            upperTimestamp);

        LastPulledFromReplicaId_ = queueReplicaId;
        return std::tuple{queueReplicaId, queueReplicaInfo, upperTimestamp};
    }

    return TError("Will not pull rows since no in-sync queue found");
}

void TQueueReplicaSelector::ResetLastPulledFromReplicaId()
{
    LastPulledFromReplicaId_ = NullObjectId;
}
////////////////////////////////////////////////////////////////////////////////

} // namspace NYT::NTabletNode
