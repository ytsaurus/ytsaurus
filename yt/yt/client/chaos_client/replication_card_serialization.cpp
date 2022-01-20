#include "replication_card_serialization.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChaosClient {

using namespace NTransactionClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSerializableReplicationProgress)

struct TSerializableReplicationProgress
    : public NYTree::TYsonSerializable
{
    struct TSerializableSegment
        : public NYTree::TYsonSerializable
    {
        TUnversionedOwningRow LowerKey;
        TTimestamp Timestamp;

        TSerializableSegment()
        {
            RegisterParameter("lower_key", LowerKey)
                .Default();
            RegisterParameter("timestamp", Timestamp)
                .Default();
        }
    };

    std::vector<TIntrusivePtr<TSerializableSegment>> Segments;
    TUnversionedOwningRow UpperKey;

    TSerializableReplicationProgress()
    {
        RegisterParameter("segments", Segments)
            .Default();
        RegisterParameter("upper_key", UpperKey)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableReplicationProgress)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSerializableReplicaInfo)

struct TSerializableReplicaInfo
    : public NYTree::TYsonSerializable
{
    TString ClusterName;
    NYPath::TYPath ReplicaPath;
    EReplicaContentType ContentType;
    EReplicaMode Mode;
    EReplicaState State;
    TReplicationProgress ReplicationProgress;

    TSerializableReplicaInfo()
    {
        RegisterParameter("cluster_name", ClusterName)
            .NonEmpty();
        RegisterParameter("replica_path", ReplicaPath)
            .NonEmpty();
        RegisterParameter("content_type", ContentType);
        RegisterParameter("mode", Mode)
            .Default(EReplicaMode::Async);
        RegisterParameter("state", State)
            .Default(EReplicaState::Disabled);
        RegisterParameter("replication_progress", ReplicationProgress)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableReplicaInfo)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSerializableReplicationCard)

struct TSerializableReplicationCard
    : public NYTree::TYsonSerializable
{
    THashMap<TString, TReplicaInfo> Replicas;
    std::vector<NObjectClient::TCellId> CoordinatorCellIds;
    TReplicationEra Era;

    TSerializableReplicationCard()
    {
        RegisterParameter("replicas", Replicas);
        RegisterParameter("coordinator_cell_ids", CoordinatorCellIds)
            .Optional();
        RegisterParameter("era", Era)
            .Default(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableReplicationCard)

////////////////////////////////////////////////////////////////////////////////

void DeserializeImpl(TReplicationProgress& replicationProgress, TSerializableReplicationProgressPtr serializable)
{
    replicationProgress.UpperKey = std::move(serializable->UpperKey);
    replicationProgress.Segments.reserve(serializable->Segments.size());

    for (auto& segment : serializable->Segments) {
        replicationProgress.Segments.push_back({
            .LowerKey = std::move(segment->LowerKey),
            .Timestamp = segment->Timestamp
        });
    }
}

void DeserializeImpl(TReplicaInfo& replicaInfo, TSerializableReplicaInfoPtr serializable)
{
    replicaInfo.ClusterName = serializable->ClusterName;
    replicaInfo.ReplicaPath = serializable->ReplicaPath;
    replicaInfo.ContentType = serializable->ContentType;
    replicaInfo.Mode = serializable->Mode;
    replicaInfo.State = serializable->State;
    replicaInfo.ReplicationProgress = std::move(serializable->ReplicationProgress);
}

void DeserializeImpl(TReplicationCard& replicationCard, TSerializableReplicationCardPtr serializable)
{
    replicationCard.Replicas.clear();
    for (const auto& [replicaId, replicaInfo] : serializable->Replicas) {
        EmplaceOrCrash(replicationCard.Replicas, TReplicaId::FromString(replicaId), replicaInfo);
    }
    replicationCard.CoordinatorCellIds = std::move(serializable->CoordinatorCellIds);
    replicationCard.Era = serializable->Era;
}

void Deserialize(TReplicationProgress& replicationProgress, INodePtr node)
{
    DeserializeImpl(replicationProgress, ConvertTo<TSerializableReplicationProgressPtr>(node));
}

void Deserialize(TReplicaInfo& replicaInfo, INodePtr node)
{
    DeserializeImpl(replicaInfo, ConvertTo<TSerializableReplicaInfoPtr>(node));
}

void Deserialize(TReplicationCard& replicationCard, INodePtr node)
{
    DeserializeImpl(replicationCard, ConvertTo<TSerializableReplicationCardPtr>(node));
}

void Deserialize(TReplicationProgress& replicationProgress, TYsonPullParserCursor* cursor)
{
    DeserializeImpl(replicationProgress, ExtractTo<TSerializableReplicationProgressPtr>(cursor));
}

void Deserialize(TReplicaInfo& replicaInfo, TYsonPullParserCursor* cursor)
{
    DeserializeImpl(replicaInfo, ExtractTo<TSerializableReplicaInfoPtr>(cursor));
}

void Deserialize(TReplicationCard& replicationCard, TYsonPullParserCursor* cursor)
{
    DeserializeImpl(replicationCard, ExtractTo<TSerializableReplicationCardPtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReplicationProgress& replicationProgress, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("segments").DoListFor(replicationProgress.Segments, [] (auto fluent, const auto& segment) {
                fluent
                    .Item().BeginMap()
                        .Item("lower_key").Value(segment.LowerKey ? segment.LowerKey : EmptyKey())
                        .Item("timestamp").Value(segment.Timestamp)
                    .EndMap();
             })
            .Item("upper_key").Value(replicationProgress.UpperKey ? replicationProgress.UpperKey : EmptyKey())
        .EndMap();
}

void Serialize(const TReplicaHistoryItem& replicaHistoryItem, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("era").Value(replicaHistoryItem.Era)
            .Item("timestamp").Value(replicaHistoryItem.Timestamp)
            .Item("mode").Value(replicaHistoryItem.Mode)
            .Item("state").Value(replicaHistoryItem.State)
        .EndMap();
}

void Serialize(
    const TReplicaInfo& replicaInfo,
    IYsonConsumer* consumer,
    const TReplicationCardFetchOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("cluster_name").Value(replicaInfo.ClusterName)
            .Item("replica_path").Value(replicaInfo.ReplicaPath)
            .Item("content_type").Value(replicaInfo.ContentType)
            .Item("mode").Value(replicaInfo.Mode)
            .Item("state").Value(replicaInfo.State)
            .DoIf(options.IncludeProgress, [&] (auto fluent) {
                fluent
                    .Item("replication_progress").Value(replicaInfo.ReplicationProgress);
            })
            .DoIf(options.IncludeHistory, [&] (auto fluent) {
                fluent
                    .Item("history").Value(replicaInfo.History);
            })
        .EndMap();
}

void Serialize(
    const TReplicationCard& replicationCard,
    IYsonConsumer* consumer,
    const TReplicationCardFetchOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("replicas").DoMapFor(replicationCard.Replicas, [&] (auto fluent, const auto& pair) {
                fluent
                    .Item(ToString(pair.first)).Do([&] (auto fluent) {
                        Serialize(pair.second, fluent.GetConsumer(), options);
                    });
            })
            .DoIf(options.IncludeCoordinators, [&] (auto fluent) {
                fluent
                    .Item("coordinator_cell_ids").Value(replicationCard.CoordinatorCellIds);
            })
            .Item("era").Value(replicationCard.Era)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NChaosClient::NProto::TReplicationProgress::TSegment* protoSegment, const TReplicationProgress::TSegment& segment)
{
    ToProto(protoSegment->mutable_lower_key(), segment.LowerKey);
    protoSegment->set_timestamp(segment.Timestamp);
}

void FromProto(TReplicationProgress::TSegment* segment, const NChaosClient::NProto::TReplicationProgress::TSegment& protoSegment)
{
    segment->LowerKey = FromProto<TUnversionedOwningRow>(protoSegment.lower_key());
    segment->Timestamp = protoSegment.timestamp();
}

void ToProto(NChaosClient::NProto::TReplicationProgress* protoReplicationProgress, const TReplicationProgress& replicationProgress)
{
    ToProto(protoReplicationProgress->mutable_segments(), replicationProgress.Segments);
    ToProto(protoReplicationProgress->mutable_upper_key(), replicationProgress.UpperKey);
}

void FromProto(TReplicationProgress* replicationProgress, const NChaosClient::NProto::TReplicationProgress& protoReplicationProgress)
{
    FromProto(&replicationProgress->Segments, protoReplicationProgress.segments());
    FromProto(&replicationProgress->UpperKey, protoReplicationProgress.upper_key());
}

void ToProto(NChaosClient::NProto::TReplicaHistoryItem* protoHistoryItem, const TReplicaHistoryItem& historyItem)
{
    protoHistoryItem->set_era(historyItem.Era);
    protoHistoryItem->set_timestamp(ToProto<ui64>(historyItem.Timestamp));
    protoHistoryItem->set_mode(ToProto<i32>(historyItem.Mode));
    protoHistoryItem->set_state(ToProto<i32>(historyItem.State));
}

void FromProto(TReplicaHistoryItem* historyItem, const NChaosClient::NProto::TReplicaHistoryItem& protoHistoryItem)
{
    historyItem->Era = protoHistoryItem.era();
    historyItem->Timestamp = FromProto<TTimestamp>(protoHistoryItem.timestamp());
    historyItem->Mode = FromProto<EReplicaMode>(protoHistoryItem.mode());
    historyItem->State = FromProto<EReplicaState>(protoHistoryItem.state());
}

void ToProto(
    NChaosClient::NProto::TReplicaInfo* protoReplicaInfo,
    const TReplicaInfo& replicaInfo,
    const TReplicationCardFetchOptions& options)
{
    protoReplicaInfo->set_cluster_name(replicaInfo.ClusterName);
    protoReplicaInfo->set_replica_path(replicaInfo.ReplicaPath);
    protoReplicaInfo->set_content_type(ToProto<i32>(replicaInfo.ContentType));
    protoReplicaInfo->set_mode(ToProto<i32>(replicaInfo.Mode));
    protoReplicaInfo->set_state(ToProto<i32>(replicaInfo.State));
    if (options.IncludeProgress) {
        ToProto(protoReplicaInfo->mutable_progress(), replicaInfo.ReplicationProgress);
    }
    if (options.IncludeHistory) {
        ToProto(protoReplicaInfo->mutable_history(), replicaInfo.History);
    }
}

void FromProto(TReplicaInfo* replicaInfo, const NChaosClient::NProto::TReplicaInfo& protoReplicaInfo)
{
    replicaInfo->ClusterName = protoReplicaInfo.cluster_name();
    replicaInfo->ReplicaPath = protoReplicaInfo.replica_path();
    replicaInfo->ContentType = FromProto<EReplicaContentType>(protoReplicaInfo.content_type());
    replicaInfo->Mode = FromProto<EReplicaMode>(protoReplicaInfo.mode());
    replicaInfo->State = FromProto<EReplicaState>(protoReplicaInfo.state());
    if (protoReplicaInfo.has_progress()) {
        FromProto(&replicaInfo->ReplicationProgress, protoReplicaInfo.progress());
    }
    FromProto(&replicaInfo->History, protoReplicaInfo.history());
}

void ToProto(
    NChaosClient::NProto::TReplicationCard* protoReplicationCard,
    const TReplicationCard& replicationCard,
    const TReplicationCardFetchOptions& options)
{
    protoReplicationCard->set_era(replicationCard.Era);
    for (const auto& [replicaId, replicaInfo] : SortHashMapByKeys(replicationCard.Replicas)) {
        auto* protoReplicaEntry = protoReplicationCard->add_replicas();
        ToProto(protoReplicaEntry->mutable_id(), replicaId);
        ToProto(protoReplicaEntry->mutable_info(), replicaInfo, options);
    }
    if (options.IncludeCoordinators) {
        ToProto(protoReplicationCard->mutable_coordinator_cell_ids(), replicationCard.CoordinatorCellIds);
    }
}

void FromProto(TReplicationCard* replicationCard, const NChaosClient::NProto::TReplicationCard& protoReplicationCard)
{
    for (const auto& protoEntry : protoReplicationCard.replicas()) {
        auto replicaId = FromProto<TReplicaId>(protoEntry.id());
        auto& replicaInfo = EmplaceOrCrash(replicationCard->Replicas, replicaId, TReplicaInfo())->second;
        FromProto(&replicaInfo, protoEntry.info());
    }
    FromProto(&replicationCard->CoordinatorCellIds, protoReplicationCard.coordinator_cell_ids());
    replicationCard->Era = protoReplicationCard.era();
}

void ToProto(
    NChaosClient::NProto::TReplicationCardFetchOptions* protoOptions,
    const TReplicationCardFetchOptions& options)
{
    protoOptions->set_include_coordinators(options.IncludeCoordinators);
    protoOptions->set_include_progress(options.IncludeProgress);
    protoOptions->set_include_history(options.IncludeHistory);
}

void FromProto(
    TReplicationCardFetchOptions* options,
    const NChaosClient::NProto::TReplicationCardFetchOptions& protoOptions)
{
    options->IncludeCoordinators = protoOptions.include_coordinators();
    options->IncludeProgress = protoOptions.include_progress();
    options->IncludeHistory = protoOptions.include_history();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
