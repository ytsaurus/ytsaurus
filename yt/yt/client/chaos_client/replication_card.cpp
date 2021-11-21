#include "replication_card.h"

#include <yt/yt/core/misc/format.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/serialize.h>

#include <util/digest/multi.h>

namespace NYT::NChaosClient {

using namespace NTransactionClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

bool IsStableReplicaMode(EReplicaMode mode)
{
    return mode == EReplicaMode::Sync || mode == EReplicaMode::Async;
}

bool IsStableReplicaState(EReplicaState state)
{
    return state == EReplicaState::Enabled || state == EReplicaState::Disabled;
}

////////////////////////////////////////////////////////////////////////////////

TReplicationCardToken::TReplicationCardToken(
    NObjectClient::TCellId chaosCellId,
    TReplicationCardId replicationCardId)
    : ChaosCellId(chaosCellId)
    , ReplicationCardId(replicationCardId)
{ }

TReplicationCardToken::operator size_t() const
{
    return MultiHash(
        ChaosCellId,
        ReplicationCardId);
}

bool TReplicationCardToken::operator == (const TReplicationCardToken& other) const
{
    return ChaosCellId == other.ChaosCellId && ReplicationCardId == other.ReplicationCardId;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReplicationProgress& replicationProgress, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Segments: [%v], UpperKey: %v}",
        MakeFormattableView(replicationProgress.Segments, [] (auto* builder, const auto& segment) {
            builder->AppendFormat("<%v, %llx>", segment.LowerKey, segment.Timestamp);
        }),
        replicationProgress.UpperKey);
}

TString ToString(const TReplicationProgress& replicationProgress)
{
    return ToStringViaBuilder(replicationProgress);
}

void FormatValue(TStringBuilderBase* builder, const TReplicaInfo& replicaInfo, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ReplicaId: %v, Cluster: %v, Path: %v, ContentType: %v, Mode: %v, State: %v, Progress: %v}",
        replicaInfo.ReplicaId,
        replicaInfo.Cluster,
        replicaInfo.TablePath,
        replicaInfo.ContentType,
        replicaInfo.Mode,
        replicaInfo.State,
        replicaInfo.ReplicationProgress);
}

TString ToString(const TReplicaInfo& replicaInfo)
{
    return ToStringViaBuilder(replicaInfo);
}

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Era: %v, Replicas: %v, CoordinatorCellIds: %v}",
        replicationCard.Era,
        replicationCard.Replicas,
        replicationCard.CoordinatorCellIds);
}

TString ToString(const TReplicationCard& replicationCard)
{
    return ToStringViaBuilder(replicationCard);
}

void FormatValue(TStringBuilderBase* builder, const TReplicationCardToken& replicationCardToken, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ChaosCellId: %v, ReplicationCardId: %v}",
        replicationCardToken.ChaosCellId,
        replicationCardToken.ReplicationCardId);
}

TString ToString(const TReplicationCardToken& replicationCardToken)
{
    return ToStringViaBuilder(replicationCardToken);
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationProgress::TSegment::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, LowerKey);
    Persist(context, Timestamp);
}

void TReplicationProgress::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Segments);
    Persist(context, UpperKey);
}

void TReplicaHistoryItem::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Era);
    Persist(context, Timestamp);
    Persist(context, Mode);
    Persist(context, State);
}

void TReplicaInfo::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Cluster);
    Persist(context, TablePath);
    Persist(context, ContentType);
    Persist(context, Mode);
    Persist(context, State);
    Persist(context, History);
    Persist(context, ReplicationProgress);
}

////////////////////////////////////////////////////////////////////////////////

void UpdateReplicationProgress(TReplicationProgress* progress, const TReplicationProgress& update)
{
    std::vector<TReplicationProgress::TSegment> segments;
    auto progressIt = progress->Segments.begin();
    auto progressEnd = progress->Segments.end();
    auto updateIt = update.Segments.begin();
    auto updateEnd = update.Segments.end();
    auto progressTimestamp = NullTimestamp;
    auto updateTimestamp = NullTimestamp;

    auto append = [&] (TUnversionedOwningRow key) {
        auto timestamp = std::max(progressTimestamp, updateTimestamp);
        if (segments.empty() || segments.back().Timestamp != timestamp) {
            segments.push_back({std::move(key), timestamp});
        }
    };

    bool upper = false;
    auto processUpperKey = [&] (const TUnversionedOwningRow& key) {
        if (upper || updateIt != updateEnd) {
            return;
        }

        auto cmpResult = CompareRows(key, update.UpperKey);
        if (cmpResult >= 0) {
            updateTimestamp = NullTimestamp;
            upper = true;
        }
        if (cmpResult > 0) {
            append(update.UpperKey);
        }
    };

    while (progressIt < progressEnd || updateIt < updateEnd) {
        int cmpResult;
        if (updateIt == updateEnd) {
            cmpResult = -1;
        } else if (progressIt == progressEnd) {
            cmpResult = 1;
        } else {
            cmpResult = CompareRows(progressIt->LowerKey, updateIt->LowerKey);
        }

        if (cmpResult < 0) {
            processUpperKey(progressIt->LowerKey);
            progressTimestamp = progressIt->Timestamp;
            append(std::move(progressIt->LowerKey));
            ++progressIt;
        } else if (cmpResult > 0) {
            updateTimestamp = updateIt->Timestamp;
            append(updateIt->LowerKey);
            ++updateIt;
        } else {
            updateTimestamp = updateIt->Timestamp;
            progressTimestamp = progressIt->Timestamp;
            append(std::move(progressIt->LowerKey));
            ++progressIt;
            ++updateIt;
        }
    }

    progress->Segments = std::move(segments);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
