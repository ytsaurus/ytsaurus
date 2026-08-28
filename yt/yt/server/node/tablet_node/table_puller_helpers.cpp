#include "table_puller_helpers.h"

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

    YT_TLOG_TRACE("Banned replica tracker checking replica")
        .With("ReplicaId", replicaId)
        .With("Result", result);

    return result;
}

void TBannedReplicaTracker::BanReplica(TReplicaId replicaId, TError error)
{
    BannedReplicas_[replicaId] = TBanInfo{ReplicaBanDuration_.value_or(std::size(BannedReplicas_)), std::move(error)};

    YT_TLOG_DEBUG("Banned replica tracker has banned replica")
        .With("ReplicaId", replicaId)
        .With("ReplicasSize", BannedReplicas_.size());
}

void TBannedReplicaTracker::SyncReplicas(const TReplicationCardPtr& replicationCard)
{
    DropMissingKeys(BannedReplicas_, replicationCard->Replicas);

    for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
        if (replicaInfo.ContentType == ETableReplicaContentType::Queue &&
            IsReplicaEnabled(replicaInfo.State) &&
            !BannedReplicas_.contains(replicaId))
        {
            EmplaceOrCrash(BannedReplicas_, replicaId, TBanInfo{0, TError()});
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
    std::optional<int> replicaBanDuration,
    bool forceSameClusterQueue)
    : Logger(logger)
    , ForceSameClusterQueue_(forceSameClusterQueue)
    , BannedReplicaTracker_(std::move(logger), replicaBanDuration)
    , LastPulledFromReplicaId_(NullObjectId)
    , NextPermittedTimeForProgressBehindAlert_(Now())
{ }

TQueueReplicaSelector::TReplicaOrError TQueueReplicaSelector::PickQueueReplica(
    TReplicaId selfUpstreamReplicaId,
    const TReplicationCardPtr& replicationCard,
    const TReplicationProgress& replicationProgress,
    const THashSet<std::string>& extraSameDcQueueClusters,
    TInstant now)
{
    // If our progress is less than any queue replica progress, pull from that replica.
    // Otherwise pull from sync replica of oldest era corresponding to our progress.

    YT_TLOG_DEBUG("Pick replica to pull from");

    auto* selfReplica = replicationCard->FindReplica(selfUpstreamReplicaId);
    if (!selfReplica) {
        return TError("Will not pull rows since replication card does not contain us");
    }

    if (!IsReplicationProgressGreaterOrEqual(replicationProgress, selfReplica->ReplicationProgress)) {
        // TODO(ponasenko-rs): Remove alerts after testing period.
        if (now >= NextPermittedTimeForProgressBehindAlert_) {
            YT_TLOG_ALERT("Will not pull rows since actual replication progress is behind replication card replica progress")
                .With("ReplicationProgress", replicationProgress)
                .With("ReplicaInfo", *selfReplica);
            NextPermittedTimeForProgressBehindAlert_ = now + TDuration::Days(1);
        }

        return TError(
            "Will not pull rows since actual replication progress is behind replication card replica progress")
            .With("replication_progress", replicationProgress)
            .With("replica_info", *selfReplica);
    }

    auto oldestTimestamp = GetReplicationProgressMinTimestamp(replicationProgress);
    auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);
    if (historyItemIndex == -1) {
        return TError("Will not pull rows since replica history does not cover replication progress")
            .With("oldest_timestamp", oldestTimestamp)
            .With("history", selfReplica->History);
    }

    YT_VERIFY(historyItemIndex >= 0 && historyItemIndex < std::ssize(selfReplica->History));
    const auto& historyItem = selfReplica->History[historyItemIndex];
    if (historyItem.IsSync()) {
        return TError("Will not pull rows since oldest progress timestamp corresponds to sync history item")
            .With("oldest_timestamp", oldestTimestamp)
            .With("history_item", historyItem);
    }

    if (!IsReplicaAsync(selfReplica->Mode)) {
        YT_TLOG_DEBUG("Pulling rows while replica is not async")
            .With("ReplicaMode", selfReplica->Mode);
        // NB: Allow this since sync replica could be catching up.
    }

    auto findFreshQueueReplica = [&] () -> std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*> {
        std::vector<std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*>> candidates;
        std::vector<std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*>> sameDcCandidates;
        std::optional<std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*>> lastFetchedCandidate;

        bool isSelfReplicaInLastEra = oldestTimestamp >= selfReplica->History.back().Timestamp;

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
                if (ForceSameClusterQueue_ && isSelfReplicaInLastEra) {
                    if (selfReplica->ClusterName == replicaInfo.ClusterName)
                    {
                        return {replicaId, &replicaInfo};
                    } else if (extraSameDcQueueClusters.contains(replicaInfo.ClusterName)) {
                        sameDcCandidates.emplace_back(replicaId, &replicaInfo);
                    }
                }

                if (!IsReplicationProgressGreaterOrEqual(replicationProgress, replicaInfo.ReplicationProgress)) {
                    if (replicaId == LastPulledFromReplicaId_) {
                        lastFetchedCandidate = {replicaId, &replicaInfo};
                    } else if (!lastFetchedCandidate) {
                        candidates.emplace_back(replicaId, &replicaInfo);
                    }
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

        if (!sameDcCandidates.empty()) {
            return sameDcCandidates[RandomNumber(sameDcCandidates.size())];
        }

        if (lastFetchedCandidate) {
            return *lastFetchedCandidate;
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
        YT_TLOG_DEBUG("Pull rows from fresh replica")
            .With("ReplicaId", queueReplicaId);

        LastPulledFromReplicaId_ = queueReplicaId;
        return std::tuple{queueReplicaId, queueReplica, NullTimestamp};
    }

    if (auto [queueReplicaId, queueReplicaInfo, upperTimestamp] = findSyncQueueReplica(); queueReplicaInfo) {
        YT_TLOG_DEBUG("Pull rows from sync replica")
            .With("ReplicaId", queueReplicaId)
            .With("OldestTimestamp", oldestTimestamp)
            .With("UpperTimestamp", upperTimestamp);

        LastPulledFromReplicaId_ = queueReplicaId;
        return std::tuple{queueReplicaId, queueReplicaInfo, upperTimestamp};
    }

    return TError("Will not pull rows since no in-sync queue found");
}

void TQueueReplicaSelector::ResetLastPulledFromReplicaId()
{
    LastPulledFromReplicaId_ = NullObjectId;
}

TBannedReplicaTracker& TQueueReplicaSelector::GetBannedReplicaTracker()
{
    return BannedReplicaTracker_;
}

////////////////////////////////////////////////////////////////////////////////

TIterationTimeTracker::TIterationTimeTracker(int previousIterationWeight, int currentIterationWeight, TDuration initialDuration)
    : PreviousIterationWeight_(previousIterationWeight)
    , CurrentIterationWeight_(currentIterationWeight)
    , SmoothedItetationDuration_(initialDuration)
{
    YT_VERIFY(PreviousIterationWeight_ >= 0);
    YT_VERIFY(CurrentIterationWeight_ > 0);
}

TDuration TIterationTimeTracker::CalculateSmoothedIterationDuration(TInstant currentIterationInstant)
{
    if (LastIterationInstant_ != TInstant::Zero()) {
        auto elapsedTime = currentIterationInstant - LastIterationInstant_;

        int weightSum = PreviousIterationWeight_ + CurrentIterationWeight_;
        auto weigthedElapsedTime = elapsedTime * CurrentIterationWeight_;
        auto weigthedPreviousTime = SmoothedItetationDuration_ * CurrentIterationWeight_;

        SmoothedItetationDuration_ = (weigthedElapsedTime + weigthedPreviousTime) / weightSum;
    }

    LastIterationInstant_ = currentIterationInstant;

    return SmoothedItetationDuration_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
