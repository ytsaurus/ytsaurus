#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////

struct TReplicationProgress
{
    struct TSegment
    {
        NTableClient::TUnversionedOwningRow LowerKey;
        NTransactionClient::TTimestamp Timestamp;

        void Persist(const TStreamPersistenceContext& context);
    };

    std::vector<TSegment> Segments;
    NTableClient::TUnversionedOwningRow UpperKey;

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicaHistoryItem
{
    NChaosClient::TReplicationEra Era;
    NTransactionClient::TTimestamp Timestamp;
    NTabletClient::ETableReplicaMode Mode;
    NTabletClient::ETableReplicaState State;

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicaInfo
{
    TString ClusterName;
    NYPath::TYPath ReplicaPath;
    NTabletClient::ETableReplicaContentType ContentType;
    NTabletClient::ETableReplicaMode Mode;
    NTabletClient::ETableReplicaState State;
    TReplicationProgress ReplicationProgress;
    std::vector<TReplicaHistoryItem> History;

    //! Returns index of history item corresponding to timestamp, -1 if none.
    int FindHistoryItemIndex(NTransactionClient::TTimestamp timestamp);

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicationCard
    : public TRefCounted
{
    THashMap<TReplicaId, TReplicaInfo> Replicas;
    std::vector<NObjectClient::TCellId> CoordinatorCellIds;
    TReplicationEra Era;
    NTableClient::TTableId TableId;
    NYPath::TYPath TablePath;
    TString TableClusterName;

    //! Returns pointer to replica with a given id, nullptr if none.
    TReplicaInfo* FindReplica(TReplicaId replicaId);
    TReplicaInfo* GetReplicaOrThrow(TReplicaId replicaId, TReplicationCardId replicationCardId);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCard)

///////////////////////////////////////////////////////////////////////////////

struct TReplicationCardFetchOptions
{
    bool IncludeCoordinators = false;
    bool IncludeProgress = false;
    bool IncludeHistory = false;

    operator size_t() const;
    bool operator == (const TReplicationCardFetchOptions& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCardFetchOptions& options, TStringBuf /*spec*/);
TString ToString(const TReplicationCardFetchOptions& options);

///////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReplicationProgress& replicationProgress, TStringBuf /*spec*/);
TString ToString(const TReplicationProgress& replicationProgress);

void FormatValue(TStringBuilderBase* builder, const TReplicaHistoryItem& replicaHistoryItem, TStringBuf /*spec*/);
TString ToString(const TReplicaHistoryItem& replicaHistoryItem);

void FormatValue(TStringBuilderBase* builder, const TReplicaInfo& replicaInfo, TStringBuf /*spec*/);
TString ToString(const TReplicaInfo& replicaInfo);

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/);
TString ToString(const TReplicationCard& replicationCard);

////////////////////////////////////////////////////////////////////////////////

bool IsReplicaReallySync(NTabletClient::ETableReplicaMode mode, NTabletClient::ETableReplicaState state);

void UpdateReplicationProgress(TReplicationProgress* progress, const TReplicationProgress& update);

bool IsReplicationProgressGreaterOrEqual(const TReplicationProgress& progress, const TReplicationProgress& other);
bool IsReplicationProgressGreaterOrEqual(const TReplicationProgress& progress, NTransactionClient::TTimestamp timestamp);

TReplicationProgress AdvanceReplicationProgress(const TReplicationProgress& progress, NTransactionClient::TTimestamp timestamp);
TReplicationProgress LimitReplicationProgressByTimestamp(const TReplicationProgress& progress, NTransactionClient::TTimestamp timestamp);

NTransactionClient::TTimestamp GetReplicationProgressMinTimestamp(const TReplicationProgress& progress);
NTransactionClient::TTimestamp GetReplicationProgressTimestampForKey(const TReplicationProgress& progress, NTableClient::TUnversionedRow key);
NTransactionClient::TTimestamp GetReplicationProgressMinTimestamp(
    const TReplicationProgress& progress,
    NTableClient::TLegacyKey lower,
    NTableClient::TLegacyKey upper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
